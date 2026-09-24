#include <cflow/adapters.h>
#include <cflow/effect.h>
#include <cflow/function_projection.h>
#include <cflow/lower.h>
#include <cflow/opt.h>
#include <cflow/verify.h>

#include "tinytest.h"

#include <stdbool.h>
#include <stddef.h>

enum {
  FLOWMQ_Q_VALIDATED = 1 << 0,
  FLOWMQ_Q_ROUTED = 1 << 1,
  FLOWMQ_Q_LOCAL_CAPACITY = 1 << 2,
  FLOWMQ_Q_REMOTE_CREDIT = 1 << 3,
  FLOWMQ_Q_ENCODED = 1 << 4,
  FLOWMQ_Q_ADMITTED = 1 << 5,
  FLOWMQ_Q_CREDIT_COMMITTED = 1 << 6,
  FLOWMQ_Q_IO_SUBMITTED = 1 << 7,
  FLOWMQ_Q_INVALID = 1 << 30,
  FLOWMQ_Q_COMPLETE = FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                      FLOWMQ_Q_LOCAL_CAPACITY | FLOWMQ_Q_REMOTE_CREDIT |
                      FLOWMQ_Q_ENCODED | FLOWMQ_Q_ADMITTED |
                      FLOWMQ_Q_CREDIT_COMMITTED | FLOWMQ_Q_IO_SUBMITTED
};

static int flowmq_q_step(int state, int expected, int next_bit) {
  return state == expected ? state | next_bit : state | FLOWMQ_Q_INVALID;
}

FunctionDecl(value, int, flowmq_q_validate,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_validate(int state) {
  return flowmq_q_step(state, 0, FLOWMQ_Q_VALIDATED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_validate);

FunctionDecl(stateful, int, flowmq_q_route,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_route(int state) {
  return flowmq_q_step(state, FLOWMQ_Q_VALIDATED, FLOWMQ_Q_ROUTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_route);

FunctionDecl(stateful, int, flowmq_q_route_mock,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_route_mock(int state) {
  return flowmq_q_step(state, FLOWMQ_Q_VALIDATED, FLOWMQ_Q_ROUTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_route_mock);

FunctionDecl(stateful, int, flowmq_q_local_capacity,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_local_capacity(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED,
                       FLOWMQ_Q_LOCAL_CAPACITY);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_local_capacity);

FunctionDecl(stateful, int, flowmq_q_remote_credit,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_remote_credit(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                           FLOWMQ_Q_LOCAL_CAPACITY,
                       FLOWMQ_Q_REMOTE_CREDIT);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_remote_credit);

FunctionDecl(fallible, int, flowmq_q_encode,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_encode(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                           FLOWMQ_Q_LOCAL_CAPACITY | FLOWMQ_Q_REMOTE_CREDIT,
                       FLOWMQ_Q_ENCODED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_encode);

FunctionDecl(stateful, int, flowmq_q_admit,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_admit(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                           FLOWMQ_Q_LOCAL_CAPACITY | FLOWMQ_Q_REMOTE_CREDIT |
                           FLOWMQ_Q_ENCODED,
                       FLOWMQ_Q_ADMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_admit);

FunctionDecl(stateful, int, flowmq_q_commit_credit,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_commit_credit(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                           FLOWMQ_Q_LOCAL_CAPACITY | FLOWMQ_Q_REMOTE_CREDIT |
                           FLOWMQ_Q_ENCODED | FLOWMQ_Q_ADMITTED,
                       FLOWMQ_Q_CREDIT_COMMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_commit_credit);

FunctionDecl(io, int, flowmq_q_submit_io,
             (int, state, CMETA_PARAM_IN));
int flowmq_q_submit_io(int state) {
  return flowmq_q_step(state,
                       FLOWMQ_Q_VALIDATED | FLOWMQ_Q_ROUTED |
                           FLOWMQ_Q_LOCAL_CAPACITY | FLOWMQ_Q_REMOTE_CREDIT |
                           FLOWMQ_Q_ENCODED | FLOWMQ_Q_ADMITTED |
                           FLOWMQ_Q_CREDIT_COMMITTED,
                       FLOWMQ_Q_IO_SUBMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_submit_io);

static bool flowmq_q_add(
    cflow_graph *graph,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable callable) {
  cflow_function_projection projection = {0};
  return cflow_function_projection_admit(
             function, abi, callable, CFLOW_OP_MAP, &projection) ==
             CFLOW_FUNCTION_PROJECTION_OK &&
         cflow_graph_add_function_projection(graph, &projection);
}

#define FLOWMQ_Q_ADD(graph, name)                                             \
  flowmq_q_add((graph), FunctionMeta(name), FunctionAbi(name),                \
               CFLOW_REFLECTED_CALLABLE(name))

static bool flowmq_q_build_valid(cflow_graph *graph, bool use_mock_route) {
  cflow_graph_init(graph, &cmeta_type_int);
  if (graph->root == CMETA_INVALID_ID) return false;
  if (!FLOWMQ_Q_ADD(graph, flowmq_q_validate)) return false;
  if (use_mock_route) {
    cflow_function_projection projection = {0};
    if (cflow_function_projection_admit(
            FunctionMeta(flowmq_q_route),
            FunctionAbi(flowmq_q_route),
            CFLOW_REFLECTED_CALLABLE(flowmq_q_route_mock),
            CFLOW_OP_MAP, &projection) != CFLOW_FUNCTION_PROJECTION_OK)
      return false;
    if (!cflow_graph_add_function_projection(graph, &projection)) return false;
  } else if (!FLOWMQ_Q_ADD(graph, flowmq_q_route)) {
    return false;
  }
  return FLOWMQ_Q_ADD(graph, flowmq_q_local_capacity) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_remote_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_encode) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_admit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_commit_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_submit_io);
}

static bool flowmq_q_build_commit_before_admit(cflow_graph *graph) {
  cflow_graph_init(graph, &cmeta_type_int);
  if (graph->root == CMETA_INVALID_ID) return false;
  return FLOWMQ_Q_ADD(graph, flowmq_q_validate) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_route) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_local_capacity) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_remote_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_encode) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_commit_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_admit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_submit_io);
}

static bool flowmq_q_result_is(const cflow_result *result, int expected) {
  return result != NULL && result->count == 1u &&
         cmeta_type_equal(result->type, &cmeta_type_int) &&
         result->data != NULL && *(const int *)result->data == expected;
}

static bool flowmq_q_graph_accepts(const cflow_graph *graph,
                                   cflow_verify_report *report) {
  const int initial = 0;
  cflow_result result = {0};
  bool ok = cflow_verify_pipeline(graph, &initial, 1u, report) &&
            cflow_eval_array(graph, &initial, 1u, &result) &&
            flowmq_q_result_is(&result, FLOWMQ_Q_COMPLETE);
  cflow_result_destroy(&result);
  return ok;
}

spec("FlowMQ CFlow control-plane qualification") {
  it("projects the canonical send ordering with exact CMeta ABI adapters") {
    cflow_graph graph = {0};
    cflow_verify_report report = {0};
    cmeta_effects effects;

    check_true(flowmq_q_build_valid(&graph, false));
    check_true(cflow_graph_validate(&graph, NULL));
    effects = cflow_graph_effects(&graph);
    check_true((effects & CMETA_EFFECT_STATEFUL) != 0u);
    check_true((effects & CMETA_EFFECT_MAY_FAIL) != 0u);
    check_true((effects & CMETA_EFFECT_IO) != 0u);
    check_true(flowmq_q_graph_accepts(&graph, &report));
    check_null(report.error);

    cflow_graph_destroy(&graph);
  }

  it("rejects commit-before-admission through the qualification state") {
    cflow_graph graph = {0};
    cflow_verify_report report = {0};
    const int initial = 0;
    cflow_result result = {0};

    check_true(flowmq_q_build_commit_before_admit(&graph));
    check_true(cflow_verify_pipeline(&graph, &initial, 1u, &report));
    check_true(cflow_eval_array(&graph, &initial, 1u, &result));
    check_true((*(const int *)result.data & FLOWMQ_Q_INVALID) != 0);
    check_false(flowmq_q_graph_accepts(&graph, &report));

    cflow_result_destroy(&result);
    cflow_graph_destroy(&graph);
  }

  it("keeps STATEFUL and IO stages as optimization barriers") {
    cflow_graph surface = {0};
    cflow_graph normalized = {0};
    cflow_graph optimized = {0};
    cflow_opt_stats stats = {0};
    const int initial = 0;
    cflow_result before = {0};
    cflow_result after = {0};

    normalized.root = CMETA_INVALID_ID;
    optimized.root = CMETA_INVALID_ID;
    check_true(flowmq_q_build_valid(&surface, false));
    check_true(cflow_graph_normalize(&normalized, &surface));
    check_true(cflow_graph_optimize(
        &optimized, &normalized,
        (cflow_opt_options){CMETA_OPT_DEFAULT}, &stats));
    check_true(stats.effect_blocked_map_fusions != 0u);
    check_true((cflow_graph_effects(&optimized) & CMETA_EFFECT_STATEFUL) != 0u);
    check_true((cflow_graph_effects(&optimized) & CMETA_EFFECT_IO) != 0u);

    check_true(cflow_eval_array(&surface, &initial, 1u, &before));
    check_true(cflow_eval_array(&optimized, &initial, 1u, &after));
    check_true(flowmq_q_result_is(&before, FLOWMQ_Q_COMPLETE));
    check_true(flowmq_q_result_is(&after, FLOWMQ_Q_COMPLETE));
    check_true(cflow_result_equal(&before, &after));

    cflow_result_destroy(&before);
    cflow_result_destroy(&after);
    cflow_graph_destroy(&optimized);
    cflow_graph_destroy(&normalized);
    cflow_graph_destroy(&surface);
  }

  it("swaps an equivalent mock adapter without changing semantic topology") {
    cflow_graph local_graph = {0};
    cflow_graph mock_graph = {0};
    cflow_verify_report local_report = {0};
    cflow_verify_report mock_report = {0};
    cflow_function_projection rejected = {0};

    check_true(flowmq_q_build_valid(&local_graph, false));
    check_true(flowmq_q_build_valid(&mock_graph, true));
    check_true(flowmq_q_graph_accepts(&local_graph, &local_report));
    check_true(flowmq_q_graph_accepts(&mock_graph, &mock_report));

    check_equal(
        cflow_function_projection_admit(
            FunctionMeta(flowmq_q_route),
            FunctionAbi(flowmq_q_route),
            CFLOW_REFLECTED_CALLABLE(flowmq_q_validate),
            CFLOW_OP_MAP, &rejected),
        CFLOW_FUNCTION_PROJECTION_CONTRACT_MISMATCH);

    {
      const cflow_subgraph *local =
          cflow_graph_subgraph(&local_graph, local_graph.root);
      const cflow_subgraph *mock =
          cflow_graph_subgraph(&mock_graph, mock_graph.root);
      check_not_null(local);
      check_not_null(mock);
      check_equal(local->node_count, mock->node_count);
      check_equal(local->edge_count, mock->edge_count);
      for (size_t i = 0u; i < local->node_count; ++i)
        check_equal(local->nodes[i].op, mock->nodes[i].op);
    }

    cflow_graph_destroy(&mock_graph);
    cflow_graph_destroy(&local_graph);
  }
}
