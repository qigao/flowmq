#include <cflow/adapters.h>
#include <cflow/effect.h>
#include <cflow/function_projection.h>
#include <cflow/verify.h>
#include <cmeta/function.h>

#include "tinytest.h"

#include <stdint.h>

enum flowmq_qualification_fact {
  FLOWMQ_Q_VALIDATED = 1 << 0,
  FLOWMQ_Q_ROUTE_SELECTED = 1 << 1,
  FLOWMQ_Q_LOCAL_CAPACITY_OK = 1 << 2,
  FLOWMQ_Q_REMOTE_CREDIT_OK = 1 << 3,
  FLOWMQ_Q_ENCODED = 1 << 4,
  FLOWMQ_Q_ADMITTED = 1 << 5,
  FLOWMQ_Q_CREDIT_COMMITTED = 1 << 6,
  FLOWMQ_Q_IO_SUBMITTED = 1 << 7,
  FLOWMQ_Q_COMPLETE =
      FLOWMQ_Q_VALIDATED |
      FLOWMQ_Q_ROUTE_SELECTED |
      FLOWMQ_Q_LOCAL_CAPACITY_OK |
      FLOWMQ_Q_REMOTE_CREDIT_OK |
      FLOWMQ_Q_ENCODED |
      FLOWMQ_Q_ADMITTED |
      FLOWMQ_Q_CREDIT_COMMITTED |
      FLOWMQ_Q_IO_SUBMITTED,
  FLOWMQ_Q_INVALID = -1
};

static int flowmq_q_require(int state, int required, int produced) {
  if (state == FLOWMQ_Q_INVALID || (state & required) != required)
    return FLOWMQ_Q_INVALID;
  return state | produced;
}

FunctionDecl(fallible, int, flowmq_q_validate,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_validate(int state) {
  return state == 0 ? FLOWMQ_Q_VALIDATED : FLOWMQ_Q_INVALID;
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_validate);

FunctionDecl(stateful, int, flowmq_q_select_route,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_select_route(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_VALIDATED,
                          FLOWMQ_Q_ROUTE_SELECTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_select_route);

FunctionDecl(fallible, int, flowmq_q_check_local_capacity,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_check_local_capacity(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_ROUTE_SELECTED,
                          FLOWMQ_Q_LOCAL_CAPACITY_OK);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_check_local_capacity);

FunctionDecl(fallible, int, flowmq_q_check_remote_credit,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_check_remote_credit(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_LOCAL_CAPACITY_OK,
                          FLOWMQ_Q_REMOTE_CREDIT_OK);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_check_remote_credit);

FunctionDecl(fallible, int, flowmq_q_encode,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_encode(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_REMOTE_CREDIT_OK,
                          FLOWMQ_Q_ENCODED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_encode);

FunctionDecl(stateful, int, flowmq_q_admit,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_admit(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_ENCODED,
                          FLOWMQ_Q_ADMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_admit);

FunctionDecl(stateful, int, flowmq_q_commit_credit,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_commit_credit(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_ADMITTED,
                          FLOWMQ_Q_CREDIT_COMMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_commit_credit);

FunctionDecl(io, int, flowmq_q_submit_io,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_submit_io(int state) {
  return flowmq_q_require(state, FLOWMQ_Q_CREDIT_COMMITTED,
                          FLOWMQ_Q_IO_SUBMITTED);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_submit_io);

FunctionDecl(fallible, int, flowmq_q_validate_mock,
    (int, state, CMETA_PARAM_IN));
int flowmq_q_validate_mock(int state) {
  return flowmq_q_validate(state);
}
CFLOW_REFLECTED_ADAPTER(flowmq_q_validate_mock);

typedef struct flowmq_q_stage_s {
  const cmeta_function_desc *function;
  const cmeta_function_abi_desc *abi;
  cmeta_callable callable;
} flowmq_q_stage_t;

#define FLOWMQ_Q_STAGE(name)                                                   \
  { FunctionMeta(name), FunctionAbi(name), CFLOW_REFLECTED_CALLABLE(name) }

static int flowmq_q_add(cflow_graph *graph, flowmq_q_stage_t stage) {
  cflow_function_projection projection = {0};
  if (cflow_function_projection_admit(
          stage.function, stage.abi, stage.callable, CFLOW_OP_MAP,
          &projection) != CFLOW_FUNCTION_PROJECTION_OK)
    return 0;
  return cflow_graph_add_function_projection(graph, &projection) ? 1 : 0;
}

static int flowmq_q_eval_final(const cflow_graph *graph, int *out_state) {
  cflow_result result = {0};
  const int initial = 0;
  int ok = 0;
  if (out_state == NULL) return 0;
  *out_state = FLOWMQ_Q_INVALID;
  if (!cflow_eval_array(graph, &initial, 1u, &result))
    return 0;
  if (result.count == 1u &&
      cmeta_type_equal(result.type, &cmeta_type_int)) {
    *out_state = ((const int *)result.data)[0];
    ok = 1;
  }
  cflow_result_destroy(&result);
  return ok;
}

static int flowmq_q_accepts(const cflow_graph *graph) {
  int state = FLOWMQ_Q_INVALID;
  return flowmq_q_eval_final(graph, &state) && state == FLOWMQ_Q_COMPLETE;
}

static void flowmq_q_build_valid(cflow_graph *graph) {
  cflow_graph_init(graph, &cmeta_type_int);
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_validate)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_select_route)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_local_capacity)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_remote_credit)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_encode)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_admit)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_commit_credit)));
  check_true(flowmq_q_add(graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_submit_io)));
}

spec("flowmq_cflow_qualification") {
  it("admits exact ABI reflected stages and verifies the valid send chain") {
    cflow_graph graph = {0};
    cflow_verify_report report = {0};
    cmeta_effects effects;

    flowmq_q_build_valid(&graph);

    check_true(cflow_graph_validate(&graph, NULL));
    check_true(flowmq_q_accepts(&graph));
    check_true(cflow_verify_pipeline(&graph, &(int){0}, 1u, &report));

    effects = cflow_graph_effects(&graph);
    check_true((effects & CMETA_EFFECT_STATEFUL) != 0u);
    check_true((effects & CMETA_EFFECT_IO) != 0u);
    check_true((effects & CMETA_EFFECT_MAY_FAIL) != 0u);

    check_equal(FunctionMeta(flowmq_q_validate)->effects,
                (cmeta_effects)CMETA_EFFECT_MAY_FAIL);
    check_equal(FunctionMeta(flowmq_q_admit)->effects,
                (cmeta_effects)CMETA_EFFECT_STATEFUL);
    check_equal(FunctionMeta(flowmq_q_submit_io)->effects,
                (cmeta_effects)(CMETA_EFFECT_IO | CMETA_EFFECT_MAY_FAIL));

    cflow_graph_destroy(&graph);
  }

  it("rejects admission before encode") {
    cflow_graph graph = {0};
    int state = 0;

    cflow_graph_init(&graph, &cmeta_type_int);
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_validate)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_select_route)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_admit)));

    check_true(cflow_graph_validate(&graph, NULL));
    check_true(flowmq_q_eval_final(&graph, &state));
    check_equal(state, FLOWMQ_Q_INVALID);
    check_false(flowmq_q_accepts(&graph));

    cflow_graph_destroy(&graph);
  }

  it("rejects credit commit before admission") {
    cflow_graph graph = {0};
    int state = 0;

    cflow_graph_init(&graph, &cmeta_type_int);
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_validate)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_select_route)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_local_capacity)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_remote_credit)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_encode)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_commit_credit)));

    check_true(flowmq_q_eval_final(&graph, &state));
    check_equal(state, FLOWMQ_Q_INVALID);
    check_false(flowmq_q_accepts(&graph));

    cflow_graph_destroy(&graph);
  }

  it("rejects IO submission before committed admission") {
    cflow_graph graph = {0};
    int state = 0;

    cflow_graph_init(&graph, &cmeta_type_int);
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_validate)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_select_route)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_local_capacity)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_check_remote_credit)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_encode)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_admit)));
    check_true(flowmq_q_add(&graph, (flowmq_q_stage_t)FLOWMQ_Q_STAGE(flowmq_q_submit_io)));

    check_true(flowmq_q_eval_final(&graph, &state));
    check_equal(state, FLOWMQ_Q_INVALID);
    check_false(flowmq_q_accepts(&graph));

    cflow_graph_destroy(&graph);
  }

  it("enforces reflected semantic contract compatibility at projection admission") {
    cflow_function_projection projection = {0};

    check_equal(
        cflow_function_projection_admit(
            FunctionMeta(flowmq_q_validate),
            FunctionAbi(flowmq_q_validate),
            CFLOW_REFLECTED_CALLABLE(flowmq_q_validate_mock),
            CFLOW_OP_MAP, &projection),
        CFLOW_FUNCTION_PROJECTION_OK);

    check_equal(
        cflow_function_projection_admit(
            FunctionMeta(flowmq_q_validate),
            FunctionAbi(flowmq_q_validate),
            CFLOW_REFLECTED_CALLABLE(flowmq_q_submit_io),
            CFLOW_OP_MAP, &projection),
        CFLOW_FUNCTION_PROJECTION_CONTRACT_MISMATCH);
  }
}
