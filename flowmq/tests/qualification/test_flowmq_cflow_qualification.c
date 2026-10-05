#include <cflow/adapters.h>
#include <cflow/effect.h>
#include <cflow/function_projection.h>
#include <cflow/lower.h>
#include <cflow/meta.h>
#include <cflow/opt.h>
#include <cflow/plan.h>
#include <cflow/verify.h>

#include "tinytest.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


typedef struct flowmq_q_managed_value_s {
  int *resource;
} flowmq_q_managed_value;

static size_t flowmq_q_managed_copy_attempts;
static size_t flowmq_q_managed_copies;
static size_t flowmq_q_managed_moves;
static size_t flowmq_q_managed_destroys;
static size_t flowmq_q_managed_live;
static size_t flowmq_q_managed_fail_copy_at;

static void flowmq_q_managed_reset(void) {
  flowmq_q_managed_copy_attempts = 0u;
  flowmq_q_managed_copies = 0u;
  flowmq_q_managed_moves = 0u;
  flowmq_q_managed_destroys = 0u;
  flowmq_q_managed_live = 0u;
  flowmq_q_managed_fail_copy_at = SIZE_MAX;
}

static flowmq_q_managed_value flowmq_q_managed_make(int value) {
  flowmq_q_managed_value result = {0};
  result.resource = (int *)malloc(sizeof(*result.resource));
  if (result.resource != NULL) {
    *result.resource = value;
    ++flowmq_q_managed_live;
  }
  return result;
}

static bool flowmq_q_managed_copy(void *destination_,
                                  const void *source_) {
  flowmq_q_managed_value *destination =
      (flowmq_q_managed_value *)destination_;
  const flowmq_q_managed_value *source =
      (const flowmq_q_managed_value *)source_;
  const size_t attempt = flowmq_q_managed_copy_attempts++;

  destination->resource = NULL;
  if (attempt == flowmq_q_managed_fail_copy_at) return false;
  if (source->resource != NULL) {
    destination->resource = (int *)malloc(sizeof(*destination->resource));
    if (destination->resource == NULL) return false;
    *destination->resource = *source->resource;
    ++flowmq_q_managed_live;
  }
  ++flowmq_q_managed_copies;
  return true;
}

static void flowmq_q_managed_move(void *destination_, void *source_) {
  flowmq_q_managed_value *destination =
      (flowmq_q_managed_value *)destination_;
  flowmq_q_managed_value *source = (flowmq_q_managed_value *)source_;

  destination->resource = source->resource;
  source->resource = NULL;
  ++flowmq_q_managed_moves;
}

static void flowmq_q_managed_destroy(void *value_) {
  flowmq_q_managed_value *value = (flowmq_q_managed_value *)value_;

  if (value->resource != NULL) {
    free(value->resource);
    value->resource = NULL;
    --flowmq_q_managed_live;
  }
  ++flowmq_q_managed_destroys;
}

static const cmeta_type_traits flowmq_q_managed_traits = {
    .flags = CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY,
    .copy_construct = flowmq_q_managed_copy,
    .move_construct = flowmq_q_managed_move,
    .destroy = flowmq_q_managed_destroy};

static const cmeta_type_desc flowmq_q_managed_type = {
    .name = "flowmq_q_managed_value",
    .size = sizeof(flowmq_q_managed_value),
    .align = _Alignof(flowmq_q_managed_value),
    .kind = CMETA_T_OBJECT,
    .pointee = NULL,
    .traits = &flowmq_q_managed_traits,
    .identity = NULL};

static void flowmq_q_managed_destroy_local(flowmq_q_managed_value *value) {
  flowmq_q_managed_destroy(value);
}


typedef struct flowmq_q_borrowed_capture_s {
  int *increment;
} flowmq_q_borrowed_capture;

lambda1(map, value, long, flowmq_q_borrowed_add,
        int, value, flowmq_q_borrowed_capture, capture) {
  return (long)value + (long)*capture.increment;
}

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

static bool flowmq_q_build_admit_before_encode(cflow_graph *graph) {
  cflow_graph_init(graph, &cmeta_type_int);
  if (graph->root == CMETA_INVALID_ID) return false;
  return FLOWMQ_Q_ADD(graph, flowmq_q_validate) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_route) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_local_capacity) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_remote_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_admit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_encode) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_commit_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_submit_io);
}

static bool flowmq_q_build_io_before_commit(cflow_graph *graph) {
  cflow_graph_init(graph, &cmeta_type_int);
  if (graph->root == CMETA_INVALID_ID) return false;
  return FLOWMQ_Q_ADD(graph, flowmq_q_validate) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_route) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_local_capacity) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_remote_credit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_encode) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_admit) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_submit_io) &&
         FLOWMQ_Q_ADD(graph, flowmq_q_commit_credit);
}

static bool flowmq_q_result_invalid(const cflow_result *result) {
  return result != NULL && result->count == 1u &&
         cmeta_type_equal(result->type, &cmeta_type_int) &&
         result->data != NULL &&
         (*(const int *)result->data & FLOWMQ_Q_INVALID) != 0;
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

  it("delegates managed result ownership to CFlow and canonical CMeta lifecycle") {
    flowmq_q_managed_value input;
    flowmq_q_managed_value destination = {0};
    cflow_stream stream = {0};
    cflow_plan plan = {0};
    cflow_result result = {0};
    const flowmq_q_managed_value *output;

    flowmq_q_managed_reset();
    input = flowmq_q_managed_make(41);
    check_not_null(input.resource);
    check_not_null(cflow_stream_init(&stream, &flowmq_q_managed_type));
    check_true(cflow_plan_compile_surface(&plan, &stream.graph, NULL));
    check_true(cflow_plan_eval_array(&plan, &input, 1u, &result));
    check_equal(result.count, (size_t)1u);
    check_true(cmeta_type_equal(result.type, &flowmq_q_managed_type));
    output = (const flowmq_q_managed_value *)result.data;
    check_not_null(output);
    check_not_null(output->resource);
    check_equal(*output->resource, 41);
    check_true(output->resource != input.resource);
    check_equal(flowmq_q_managed_live, (size_t)2u);

    check_true(cflow_result_move_value(
        &result, &flowmq_q_managed_type, &destination));
    check_null(result.data);
    check_equal(result.count, (size_t)0u);
    check_null(result.type);
    check_not_null(destination.resource);
    check_equal(*destination.resource, 41);
    check_equal(flowmq_q_managed_moves, (size_t)1u);
    check_equal(flowmq_q_managed_live, (size_t)2u);

    cflow_result_destroy(&result);
    flowmq_q_managed_destroy_local(&destination);
    flowmq_q_managed_destroy_local(&input);
    cflow_plan_destroy(&plan);
    cflow_stream_destroy(&stream);

    check_equal(flowmq_q_managed_live, (size_t)0u);
    check_true(flowmq_q_managed_copies >= (size_t)1u);
  }

  it("cleans partially constructed managed plan results without FlowMQ cleanup state") {
    flowmq_q_managed_value input[2];
    cflow_stream stream = {0};
    cflow_plan plan = {0};
    cflow_result result = {0};

    flowmq_q_managed_reset();
    input[0] = flowmq_q_managed_make(7);
    input[1] = flowmq_q_managed_make(14);
    check_not_null(input[0].resource);
    check_not_null(input[1].resource);
    check_not_null(cflow_stream_init(&stream, &flowmq_q_managed_type));
    check_true(cflow_plan_compile_surface(&plan, &stream.graph, NULL));

    flowmq_q_managed_fail_copy_at = 1u;
    check_false(cflow_plan_eval_array(&plan, input, 2u, &result));
    check_null(result.data);
    check_equal(result.count, (size_t)0u);
    check_null(result.type);
    check_equal(flowmq_q_managed_live, (size_t)2u);
    check_true(flowmq_q_managed_destroys >= (size_t)1u);

    cflow_result_destroy(&result);
    flowmq_q_managed_destroy_local(&input[0]);
    flowmq_q_managed_destroy_local(&input[1]);
    cflow_plan_destroy(&plan);
    cflow_stream_destroy(&stream);
    check_equal(flowmq_q_managed_live, (size_t)0u);
  }


  it("owns callable capture bytes while borrowing transitive pointer identity") {
    cflow_stream stream = {0};
    cflow_graph clone = {0};
    cflow_result result = {0};
    flowmq_q_borrowed_capture original_capture = {0};
    flowmq_q_borrowed_capture cloned_capture = {0};
    int external_increment = 10;
    const int input[] = {1, 2};
    const long expected[] = {21L, 22L};
    cflow_map_callable callable =
        flowmq_q_borrowed_add(
            (flowmq_q_borrowed_capture){&external_increment});
    const cflow_node *source_node;
    const cflow_node *clone_node;

    clone.root = CMETA_INVALID_ID;
    check_not_null(cflow_stream_init(&stream, &cmeta_type_int));
    check_not_null(stream.map(&stream, callable));
    check_true(cflow_graph_clone(&clone, &stream.graph));

    source_node = cflow_subgraph_node(
        cflow_graph_subgraph(&stream.graph, stream.graph.root), 1u);
    clone_node = cflow_subgraph_node(
        cflow_graph_subgraph(&clone, clone.root), 1u);
    check_not_null(source_node);
    check_not_null(clone_node);
    check_equal(source_node->fn.capture_size,
                sizeof(flowmq_q_borrowed_capture));
    check_equal(clone_node->fn.capture_size,
                sizeof(flowmq_q_borrowed_capture));
    check_true(&source_node->fn.capture != &clone_node->fn.capture);

    memcpy(&original_capture, source_node->fn.capture.bytes,
           sizeof(original_capture));
    memcpy(&cloned_capture, clone_node->fn.capture.bytes,
           sizeof(cloned_capture));
    check_true(original_capture.increment == &external_increment);
    check_true(cloned_capture.increment == &external_increment);

    external_increment = 20;
    cflow_stream_destroy(&stream);
    check_equal(external_increment, 20);

    check_true(cflow_eval_array(&clone, input, 2u, &result));
    check_equal(result.count, (size_t)2u);
    check_true(cmeta_type_equal(result.type, &cmeta_type_long));
    check_equal(result.data, expected, sizeof(expected));

    cflow_result_destroy(&result);
    cflow_graph_destroy(&clone);
    check_equal(external_increment, 20);
  }

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

  it("rejects admission before encode through the qualification state") {
    cflow_graph graph = {0};
    cflow_verify_report report = {0};
    const int initial = 0;
    cflow_result result = {0};

    check_true(flowmq_q_build_admit_before_encode(&graph));
    check_true(cflow_verify_pipeline(&graph, &initial, 1u, &report));
    check_true(cflow_eval_array(&graph, &initial, 1u, &result));
    check_true(flowmq_q_result_invalid(&result));
    check_false(flowmq_q_graph_accepts(&graph, &report));

    cflow_result_destroy(&result);
    cflow_graph_destroy(&graph);
  }

  it("rejects IO submission before credit commit") {
    cflow_graph graph = {0};
    cflow_verify_report report = {0};
    const int initial = 0;
    cflow_result result = {0};

    check_true(flowmq_q_build_io_before_commit(&graph));
    check_true(cflow_verify_pipeline(&graph, &initial, 1u, &report));
    check_true(cflow_eval_array(&graph, &initial, 1u, &result));
    check_true(flowmq_q_result_invalid(&result));
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
