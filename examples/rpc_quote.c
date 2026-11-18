/* A quote worker asks independent price and delivery services, then replies
 * once both results arrive. All sockets run on one loopback I/O owner. */
#include "flowmq_rpc.h"
#include <cflow/executor.h>
#include <cflow/statechart_instance.h>
#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <string.h>

enum { QUOTE, PRICE, DELIVERY, SERVICE_COUNT };
enum { FRONT, BACK, BUYER, PLANNER, FIRST_WORKER, SOCKET_COUNT = FIRST_WORKER + SERVICE_COUNT };
enum {
  MAX_QUOTES = 2,
  SCENARIO_REQUESTS = MAX_QUOTES + 1,
  BRANCHES = 2,
  QUOTE_TIMEOUT_MS = 5000,
  SHORT_TIMEOUT_MS = 1000
};
static const char *const services[] = {"quote", "price", "delivery"};
static const int rates[] = {37, 5};
static const char *const deferred[] = {"quote.total"};

enum quote_state {
  ROOT = 1,
  INITIAL,
  WAITING,
  HAVE_PRICE,
  HAVE_DELIVERY,
  TOTAL,
  FAILED,
  CANCELLED
};
enum quote_event_id { PRICE_READY = 1, DELIVERY_READY, COMPONENT_FAILED, CANCEL_QUOTE };
enum quote_transition {
  BEGIN = 1,
  FIRST_PRICE,
  FIRST_DELIVERY,
  LAST_PRICE,
  LAST_DELIVERY,
  FAIL,
  CANCEL
};
enum { ADD_AMOUNT = 1 };

static const cflow_statechart_state states[] = {
    {ROOT, 0, CFLOW_STATECHART_COMPOUND, ROOT},
    {INITIAL, ROOT, CFLOW_STATECHART_INITIAL, INITIAL},
    {WAITING, ROOT, CFLOW_STATECHART_ATOMIC, WAITING},
    {HAVE_PRICE, ROOT, CFLOW_STATECHART_ATOMIC, HAVE_PRICE},
    {HAVE_DELIVERY, ROOT, CFLOW_STATECHART_ATOMIC, HAVE_DELIVERY},
    {TOTAL, ROOT, CFLOW_STATECHART_ATOMIC, TOTAL},
    {FAILED, ROOT, CFLOW_STATECHART_ATOMIC, FAILED},
    {CANCELLED, ROOT, CFLOW_STATECHART_ATOMIC, CANCELLED},
};
static const cflow_event_type events[] = {
    {PRICE_READY, &cmeta_type_int},
    {DELIVERY_READY, &cmeta_type_int},
    {COMPONENT_FAILED, &cmeta_type_int},
    {CANCEL_QUOTE, &cmeta_type_int},
};
static const cflow_statechart_transition transitions[] = {
    {BEGIN, INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0, 0, 0, WAITING,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, BEGIN},
    {FIRST_PRICE, WAITING, CFLOW_STATECHART_TRIGGER_EVENT, PRICE_READY, 0, 0, HAVE_PRICE,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, FIRST_PRICE},
    {FIRST_DELIVERY, WAITING, CFLOW_STATECHART_TRIGGER_EVENT, DELIVERY_READY, 0, 0, HAVE_DELIVERY,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, FIRST_DELIVERY},
    {LAST_PRICE, HAVE_DELIVERY, CFLOW_STATECHART_TRIGGER_EVENT, PRICE_READY, 0, 0, TOTAL,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, LAST_PRICE},
    {LAST_DELIVERY, HAVE_PRICE, CFLOW_STATECHART_TRIGGER_EVENT, DELIVERY_READY, 0, 0, TOTAL,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, LAST_DELIVERY},
    {FAIL, ROOT, CFLOW_STATECHART_TRIGGER_EVENT, COMPONENT_FAILED, 0, 0, FAILED,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, FAIL},
    {CANCEL, ROOT, CFLOW_STATECHART_TRIGGER_EVENT, CANCEL_QUOTE, 0, 0, CANCELLED,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0, CANCEL},
};
static const cflow_statechart_executable executables[] = {
    {ADD_AMOUNT, &cmeta_type_int, 0, CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS},
};
static const cflow_statechart_transition_action actions[] = {
    {FIRST_PRICE, ADD_AMOUNT, 0},
    {FIRST_DELIVERY, ADD_AMOUNT, 0},
    {LAST_PRICE, ADD_AMOUNT, 0},
    {LAST_DELIVERY, ADD_AMOUNT, 0},
};

static bool add_amount(void *user, cflow_statechart_action_phase phase,
                       cflow_machine_state_id owner, const void *state,
                       const cflow_event_view *event, void *out_state,
                       cflow_statechart_raise_fn raise_internal, void *raise_user,
                       const char **error) {
  (void)user;
  (void)phase;
  (void)owner;
  (void)raise_internal;
  (void)raise_user;
  (void)error;
  *(int *)out_state = *(const int *)state + *(const int *)event->payload;
  return true;
}

static const cflow_statechart_executable_binding bindings[] = {{ADD_AMOUNT, add_amount}};

typedef struct quote_call {
  cflow_statechart_instance instance;
  flowmq_rpc_ticket ticket;
  bool active, cancelling;
} quote_call;

typedef struct quote_app {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sockets[SOCKET_COUNT];
  flowmq_mdp_client *buyer, *planner;
  flowmq_mdp_worker *workers[SERVICE_COUNT];
  flowmq_mdp_broker *broker;
  flowmq_rpc_engine *engine;
  flowmq_rpc_method rate_method[2];
  flowmq_rpc_server *rate_server[2];
  cflow_statechart chart;
  cflow_executor executor;
  quote_call calls[MAX_QUOTES];
  bool executor_open, concurrent;
  uint64_t scenario_now;
  size_t cancelled;
} quote_app;

static uint64_t clock_now(void *context) {
  quote_app *app = context;
  return app->concurrent ? app->scenario_now : monotonic_ms();
}

static int authorize(void *unused, int worker, const void *identity, size_t size,
                     const char *service) {
  (void)unused;
  for (size_t i = 0; i < SERVICE_COUNT; ++i) {
    if (strcmp(service, services[i])) continue;
    const char *allowed = worker ? services[i] : (i == QUOTE ? "buyer" : "planner");
    return strlen(allowed) == size && !memcmp(identity, allowed, size);
  }
  return 0;
}

static int connect_peer(flowmq_socket_t *dealer, flowmq_socket_t *router, const char *identity) {
  char address[128];
  size_t size;
  int rc = flowmq_last_endpoint(router, address, sizeof(address), &size);
  if (rc == SALTS_OK) rc = flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity, strlen(identity));
  if (rc == SALTS_OK) rc = flowmq_connect(dealer, address);
  return rc;
}

static int32_t read_amount(void *rate, const flowmq_rpc_invocation *request,
                           flowmq_rpc_output *out) {
  (void)request;
  out->value = json_create_int64(*(const int *)rate);
  return out->value ? 0 : FLOWMQ_RPC_INTERNAL_ERROR;
}

static int open_app(quote_app *app) {
  const cflow_statechart_definition definition = {
      .state_type = &cmeta_type_int,
      .states = states,
      .state_count = sizeof(states) / sizeof(*states),
      .events = events,
      .event_count = sizeof(events) / sizeof(*events),
      .executables = executables,
      .executable_count = sizeof(executables) / sizeof(*executables),
      .transitions = transitions,
      .transition_count = sizeof(transitions) / sizeof(*transitions),
      .transition_actions = actions,
      .transition_action_count = sizeof(actions) / sizeof(*actions),
  };
  cflow_statechart_status chart_status = cflow_statechart_build(&app->chart, &definition);
  if (chart_status != CFLOW_STATECHART_OK) {
    fprintf(stderr, "Statechart build failed (%d)\n", (int)chart_status);
    return SALTS_EINVAL;
  }
  if (!cflow_executor_serial_init_with_capacity(&app->executor, MAX_QUOTES * 2))
    return SALTS_ENOMEM;
  app->executor_open = true;
  app->ctx = flowmq_ctx_new();
  if (!app->ctx) return SALTS_ENOMEM;
  for (size_t i = 0; i < SOCKET_COUNT; ++i) {
    app->sockets[i] = flowmq_socket(app->ctx, i <= BACK ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
    if (!app->sockets[i]) return SALTS_ENOMEM;
  }
  int rc = flowmq_bind(app->sockets[FRONT], "tcp://127.0.0.1:0");
  if (rc == SALTS_OK) rc = flowmq_bind(app->sockets[BACK], "tcp://127.0.0.1:0");
  if (rc == SALTS_OK) rc = connect_peer(app->sockets[BUYER], app->sockets[FRONT], "buyer");
  if (rc == SALTS_OK) rc = connect_peer(app->sockets[PLANNER], app->sockets[FRONT], "planner");
  const flowmq_mdp_broker_config broker_cfg = {
      1, MAX_QUOTES * (1 + BRANCHES), SERVICE_COUNT, MAX_QUOTES * BRANCHES, 2000, authorize, NULL,
      0};
  if (rc == SALTS_OK)
    rc = flowmq_mdp_broker_open(app->sockets[FRONT], app->sockets[BACK], &broker_cfg, &app->broker);
  if (rc == SALTS_OK) rc = flowmq_mdp_client_open(app->sockets[BUYER], 2, MAX_QUOTES, &app->buyer);
  if (rc == SALTS_OK)
    rc = flowmq_mdp_client_open(app->sockets[PLANNER], 3, MAX_QUOTES * BRANCHES, &app->planner);
  for (size_t i = 0; i < SERVICE_COUNT && rc == SALTS_OK; ++i) {
    flowmq_socket_t *sock = app->sockets[FIRST_WORKER + i];
    rc = connect_peer(sock, app->sockets[BACK], services[i]);
    /* New sessions for this finite deployment. */
    if (rc == SALTS_OK)
      rc = flowmq_mdp_worker_open(sock, services[i], 4 + i, MAX_QUOTES, 100, &app->workers[i]);
  }
  for (size_t i = 0; i < 2 && rc == SALTS_OK; ++i) {
    app->rate_method[i] = (flowmq_rpc_method){"amount", read_amount, (void *)&rates[i]};
    rc = flowmq_rpc_server_open(&app->rate_method[i], 1, &app->rate_server[i]);
  }
  const flowmq_rpc_engine_config cfg = {.schedule = {.capacity = MAX_QUOTES},
                                        .deferred_methods = deferred,
                                        .deferred_count = 1,
                                        .child_capacity = MAX_QUOTES * BRANCHES,
                                        .children_per_request = BRANCHES,
                                        .now_ms = clock_now,
                                        .context = app};
  if (rc == SALTS_OK)
    rc = flowmq_rpc_engine_open(app->workers[QUOTE], NULL, app->planner, &cfg, &app->engine);
  return rc;
}

static int decode_amount(const flowmq_mdp_result *result, const flowmq_rpc_request *request,
                         int *amount) {
  if (result->outcome != FLOWMQ_MDP_RESULT) return result->status ? result->status : SALTS_EPROTO;
  json_value_t *response = NULL;
  int rc = flowmq_rpc_decode(result, request, 1, false, &response);
  if (rc == SALTS_OK) {
    const json_value_t *value = json_object_get(response, "result");
    const double n = json_number(value);
    if (!value || json_type(value) != JSON_NUMBER || !(n >= 0 && n <= 1000000) || n != (int)n)
      rc = SALTS_EPROTO;
    else *amount = (int)n;
  }
  json_free(response);
  return rc;
}

/* The executor owns statechart actions. Only this I/O owner touches the
 * engine. The mailbox copies scalar results and keeps no RPC DOM pointers. */
static int send_quote_event(quote_call *call, cflow_event_id id, int payload) {
  const cflow_event_view event = {id, &cmeta_type_int, &payload};
  cflow_mailbox_status status = cflow_statechart_instance_try_send(&call->instance, &event);
  if (status == CFLOW_MAILBOX_OK) return SALTS_OK;
  fprintf(stderr, "Statechart event admission failed (%d)\n", (int)status);
  return status == CFLOW_MAILBOX_FULL ? SALTS_ENOBUFS : SALTS_EPROTO;
}

static int start_quote(quote_app *app, quote_call *call, const flowmq_rpc_event *event) {
  if (call->active) return SALTS_EPROTO;
  const int initial_total = 0;
  const cflow_statechart_instance_config config = {
      .statechart = &app->chart,
      .initial_state = &initial_total,
      .executables = bindings,
      .executable_count = sizeof(bindings) / sizeof(*bindings),
      .external_event_capacity = BRANCHES + 1,
      .internal_event_capacity = 1,
      .completion_capacity = 1,
      .microstep_limit = 4,
      .executor = &app->executor,
  };
  cflow_statechart_instance_status status =
      cflow_statechart_instance_init(&call->instance, &config);
  if (status != CFLOW_STATECHART_INSTANCE_OK) {
    fprintf(stderr, "Statechart instance initialization failed (%d)\n", (int)status);
    int rc = flowmq_rpc_engine_finish(app->engine, event->ticket, FLOWMQ_RPC_INTERNAL_ERROR,
                                      (flowmq_rpc_output){0});
    return rc == SALTS_OK ? SALTS_EPROTO : rc;
  }
  call->ticket = event->ticket;
  call->cancelling = false;
  call->active = true;
  /* Initial stabilization commits WAITING. Start both independent branches
   * on the I/O owner after the statechart has accepted this request. */
  const flowmq_rpc_request child = {"amount", NULL, event->input.id};
  const flowmq_rpc_branch branches[] = {{services[PRICE], child, PRICE_READY},
                                        {services[DELIVERY], child, DELIVERY_READY}};
  int rc = flowmq_rpc_engine_fork(app->engine, event->ticket, branches, BRANCHES);
  if (rc == SALTS_OK) return rc;
  if (rc == SALTS_ETIMEDOUT || rc == SALTS_ESHUTDOWN) return SALTS_OK; /* CANCEL follows. */
  if (rc == SALTS_ENOBUFS) return send_quote_event(call, COMPONENT_FAILED, rc);
  return rc;
}

static int quote_event(quote_app *app, const flowmq_rpc_event *event) {
  if (event->kind == FLOWMQ_RPC_PROGRESS) return SALTS_OK;
  if (event->ticket.slot >= MAX_QUOTES) return SALTS_EPROTO;
  quote_call *call = &app->calls[event->ticket.slot];
  if (event->kind == FLOWMQ_RPC_START) return start_quote(app, call, event);
  if (!call->active || call->ticket.generation != event->ticket.generation ||
      call->ticket.member != event->ticket.member)
    return SALTS_EPROTO;
  if (event->kind == FLOWMQ_RPC_CANCEL) {
    call->cancelling = true;
    return send_quote_event(call, CANCEL_QUOTE, 0);
  }
  if (event->kind != FLOWMQ_RPC_CHILD ||
      (event->child_tag != PRICE_READY && event->child_tag != DELIVERY_READY))
    return SALTS_EPROTO;
  const flowmq_rpc_request child = {"amount", NULL, event->input.id};
  int amount;
  int rc = decode_amount(&event->child, &child, &amount);
  return rc == SALTS_OK ? send_quote_event(call, event->child_tag, amount)
                        : send_quote_event(call, COMPONENT_FAILED, rc);
}

static int settle_quotes(quote_app *app) {
  for (size_t i = 0; i < MAX_QUOTES; ++i) {
    quote_call *call = &app->calls[i];
    if (!call->active) continue;
    const char *error = cflow_statechart_instance_error(&call->instance);
    if (error) {
      fprintf(stderr, "Statechart execution failed (%s)\n", error);
      return SALTS_EPROTO;
    }
    cflow_machine_state_id state = cflow_statechart_instance_current_state(&call->instance);
    if (call->cancelling && state != CANCELLED) continue;
    if (state != TOTAL && state != FAILED && state != CANCELLED) continue;
    flowmq_rpc_output output = {0};
    int32_t code = 0;
    if (state == TOTAL) {
      /* TOTAL has no further amount transitions, so its sum is stable
       * until the owner admits cancellation or destroys this instance. */
      const cmeta_type_desc *type;
      int sum;
      if (!cflow_statechart_instance_copy_state(&call->instance, &type, &sum, sizeof(sum)))
        return SALTS_EPROTO;
      output.value = json_create_int64(sum);
      if (!output.value) code = FLOWMQ_RPC_INTERNAL_ERROR;
    } else if (state == FAILED) {
      code = FLOWMQ_RPC_INTERNAL_ERROR;
      output.message = "Quote component unavailable";
    }
    if (cflow_statechart_instance_destroy(&call->instance) != CFLOW_STATECHART_INSTANCE_OK) {
      json_free(output.value);
      return SALTS_EPROTO;
    }
    call->active = false;
    int rc = flowmq_rpc_engine_finish(app->engine, call->ticket, code, output);
    if (rc != SALTS_OK) return rc;
    if (state == CANCELLED) ++app->cancelled;
  }
  return SALTS_OK;
}

static bool both_received_delivery(const quote_app *app) {
  for (size_t i = 0; i < MAX_QUOTES; ++i)
    if (!app->calls[i].active ||
        cflow_statechart_instance_current_state(&app->calls[i].instance) != HAVE_DELIVERY)
      return false;
  return true;
}

static int io_turn(quote_app *app, flowmq_pollitem_t *items) {
  size_t ready = 0;
  int rc = flowmq_poll(items, SOCKET_COUNT, 1, &ready);
  uint64_t now = clock_now(app);
  if (rc == SALTS_OK) rc = flowmq_mdp_client_step(app->buyer, now);
  if (rc == SALTS_OK) rc = flowmq_mdp_client_step(app->planner, now);
  if (rc == SALTS_OK) rc = flowmq_mdp_broker_step(app->broker, now);
  for (size_t i = 0; i < SERVICE_COUNT && rc == SALTS_OK; ++i)
    rc = flowmq_mdp_worker_step(app->workers[i], now);
  for (size_t i = PRICE; i <= DELIVERY && rc == SALTS_OK; ++i) {
    /* Exercise delivery-first replies and cancellation without sleeping
     * across a deadline. Both price tasks can be queued during the test. */
    if (i == PRICE && app->concurrent && !app->cancelled) continue;
    rc = flowmq_rpc_execute(app->rate_server[i - PRICE], app->workers[i]);
    if (rc == SALTS_EBUSY) rc = SALTS_OK;
  }
  if (rc != SALTS_OK) return rc;
  flowmq_rpc_event event;
  rc = flowmq_rpc_engine_poll(app->engine, &event);
  if (rc == SALTS_OK) rc = quote_event(app, &event);
  else if (rc == SALTS_EBUSY) rc = SALTS_OK;
  if (rc == SALTS_OK) rc = settle_quotes(app);
  return rc;
}

static int run(quote_app *app, const flowmq_rpc_request *requests) {
  flowmq_pollitem_t items[SOCKET_COUNT];
  for (size_t i = 0; i < SOCKET_COUNT; ++i)
    items[i] = (flowmq_pollitem_t){app->sockets[i], FLOWMQ_POLLIN, 0};
  size_t count = app->concurrent ? MAX_QUOTES : 1;
  flowmq_mdp_id handles[SCENARIO_REQUESTS];
  bool finished[SCENARIO_REQUESTS] = {0};
  uint64_t started = monotonic_ms();
  for (size_t i = 0; i < count; ++i) {
    uint32_t budget = app->concurrent && i == 0 ? SHORT_TIMEOUT_MS : QUOTE_TIMEOUT_MS;
    int rc = flowmq_rpc_call(app->buyer, services[QUOTE], &requests[i], 1, false, clock_now(app),
                             budget, &handles[i]);
    if (rc != SALTS_OK) return rc;
  }
  size_t remaining = count;
  while (remaining) {
    if (monotonic_ms() - started >= QUOTE_TIMEOUT_MS + SHORT_TIMEOUT_MS) return SALTS_ETIMEDOUT;
    int rc = io_turn(app, items);
    if (rc != SALTS_OK) return rc;
    if (app->concurrent && both_received_delivery(app)) {
      /* Once both charts have processed delivery, expire just the first
       * request. The simulated advance is shorter than the broker lease. */
      app->scenario_now = 1 + SHORT_TIMEOUT_MS;
    }
    if (app->concurrent && count == MAX_QUOTES && app->cancelled && finished[0]) {
      /* Reuse the released slot while the other quote remains independent. */
      rc = flowmq_rpc_call(app->buyer, services[QUOTE], &requests[count], 1, false, clock_now(app),
                           QUOTE_TIMEOUT_MS, &handles[count]);
      if (rc != SALTS_OK) return rc;
      ++count;
      ++remaining;
    }
    for (size_t i = 0; i < count; ++i) {
      if (finished[i]) continue;
      flowmq_mdp_result reply;
      rc = flowmq_mdp_take_result(app->buyer, handles[i], &reply);
      if (rc == SALTS_EBUSY) continue;
      if (rc != SALTS_OK) return rc;
      if (app->concurrent && i == 0) {
        if (reply.outcome != FLOWMQ_MDP_UNKNOWN || reply.status != SALTS_ETIMEDOUT)
          return SALTS_EPROTO;
      } else {
        int total;
        rc = decode_amount(&reply, &requests[i], &total);
        if (rc != SALTS_OK) return rc;
        if (total != 42) return SALTS_EPROTO;
        printf("quote.total = %d\n", total);
      }
      finished[i] = true;
      --remaining;
    }
  }
  if (app->concurrent && (app->cancelled != 1 || count != SCENARIO_REQUESTS)) return SALTS_EPROTO;
  return SALTS_OK;
}

static int close_engine(quote_app *app) {
  flowmq_rpc_engine_stop(app->engine);
  int first_error = SALTS_OK;
  for (size_t i = 0; i < MAX_QUOTES; ++i) {
    quote_call *call = &app->calls[i];
    if (!call->active) continue;
    /* Quiesce the chart's pure actions before acknowledging its ticket.
     * This also handles shutdown after a CANCEL event was already emitted. */
    if (cflow_statechart_instance_destroy(&call->instance) != CFLOW_STATECHART_INSTANCE_OK)
      return SALTS_EPROTO;
    call->active = false;
    int rc = flowmq_rpc_engine_finish(app->engine, call->ticket, 0, (flowmq_rpc_output){0});
    if (first_error == SALTS_OK) first_error = rc;
  }
  int rc;
  while ((rc = flowmq_rpc_engine_close(app->engine)) == SALTS_EBUSY) {
    flowmq_rpc_event event;
    rc = flowmq_rpc_engine_poll(app->engine, &event);
    if (rc == SALTS_OK && event.kind == FLOWMQ_RPC_CANCEL)
      rc = flowmq_rpc_engine_finish(app->engine, event.ticket, 0, (flowmq_rpc_output){0});
    if (rc != SALTS_OK) return rc;
  }
  return first_error == SALTS_OK ? rc : first_error;
}

int main(int argc, char **argv) {
  bool concurrent = argc == 2 && !strcmp(argv[1], "--concurrent");
  if (argc != 1 && !concurrent) {
    fprintf(stderr, "usage: flowmq_rpc_quote [--concurrent]\n");
    return 2;
  }
  quote_app app = {.concurrent = concurrent, .scenario_now = 1};
  int rc = open_app(&app);
  json_value_t *ids[SCENARIO_REQUESTS] = {0};
  flowmq_rpc_request requests[SCENARIO_REQUESTS];
  size_t count = concurrent ? SCENARIO_REQUESTS : 1;
  for (size_t i = 0; i < count && rc == SALTS_OK; ++i) {
    ids[i] = json_create_uint64(i + 1);
    if (!ids[i]) rc = SALTS_ENOMEM;
    requests[i] = (flowmq_rpc_request){"quote.total", NULL, ids[i]};
  }
  if (rc == SALTS_OK) rc = run(&app, requests);
  for (size_t i = 0; i < count; ++i)
    json_free(ids[i]);
  if (app.engine) {
    int status = close_engine(&app);
    if (rc == SALTS_OK) rc = status;
  }
  if (app.executor_open) cflow_executor_destroy(&app.executor);
  cflow_statechart_destroy(&app.chart);
  for (size_t i = 0; i < 2; ++i)
    flowmq_rpc_server_close(app.rate_server[i]);
  for (size_t i = 0; i < SERVICE_COUNT; ++i) {
    if (!app.workers[i]) continue;
    flowmq_mdp_worker_retire(app.workers[i]);
    int status = flowmq_mdp_worker_close(app.workers[i]);
    if (rc == SALTS_OK) rc = status;
  }
  flowmq_mdp_client_close(app.planner);
  flowmq_mdp_client_close(app.buyer);
  flowmq_mdp_broker_close(app.broker);
  for (size_t i = 0; i < SOCKET_COUNT; ++i) {
    if (!app.sockets[i]) continue;
    int status = flowmq_close(app.sockets[i]);
    if (rc == SALTS_OK) rc = status;
  }
  if (app.ctx) {
    int status = flowmq_ctx_term(app.ctx);
    if (rc == SALTS_OK) rc = status;
  }
  if (rc != SALTS_OK) fprintf(stderr, "quote failed (%d)\n", rc);
  return rc == SALTS_OK ? 0 : 1;
}
