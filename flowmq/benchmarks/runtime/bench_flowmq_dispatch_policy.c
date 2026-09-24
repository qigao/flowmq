#include "flowmq_pattern.h"
#include "tinytest.h"

#include <stddef.h>
#include <stdint.h>

enum {
  FLOWMQ_DISPATCH_CASES = 1024u,
  FLOWMQ_DISPATCH_SAMPLES = 10000u
};

typedef enum flowmq_dispatch_action_e {
  FLOWMQ_DISPATCH_IDENTITY = 1,
  FLOWMQ_DISPATCH_MULTIPART,
  FLOWMQ_DISPATCH_FANOUT,
  FLOWMQ_DISPATCH_PINNED,
  FLOWMQ_DISPATCH_REPLY_PEER,
  FLOWMQ_DISPATCH_SCAN
} flowmq_dispatch_action_t;

typedef flowmq_dispatch_action_t (*flowmq_dispatch_ops_fn)(int active);

typedef struct flowmq_dispatch_case_s {
  const flowmq_pattern_desc_t *desc;
  flowmq_dispatch_ops_fn ops;
  unsigned sending_multipart : 1;
  unsigned sndmore : 1;
  unsigned send_peer_active : 1;
} flowmq_dispatch_case_t;

static flowmq_dispatch_case_t FLOWMQ_DISPATCH_CASE_DATA[FLOWMQ_DISPATCH_CASES];
static volatile uint64_t FLOWMQ_DISPATCH_SINK = 0u;

static flowmq_dispatch_action_t flowmq_dispatch_ops_fanout(int active) {
  (void)active;
  return FLOWMQ_DISPATCH_FANOUT;
}

static flowmq_dispatch_action_t flowmq_dispatch_ops_reply(int active) {
  return active ? FLOWMQ_DISPATCH_PINNED : FLOWMQ_DISPATCH_REPLY_PEER;
}

static flowmq_dispatch_action_t flowmq_dispatch_ops_scan(int active) {
  return active ? FLOWMQ_DISPATCH_PINNED : FLOWMQ_DISPATCH_SCAN;
}

static flowmq_dispatch_ops_fn
flowmq_dispatch_ops_for(const flowmq_pattern_desc_t *desc) {
  switch ((flowmq_pattern_route_class_t)desc->routing_class) {
  case FLOWMQ_PATTERN_ROUTE_FANOUT:
    return flowmq_dispatch_ops_fanout;
  case FLOWMQ_PATTERN_ROUTE_REPLY_PEER:
    return flowmq_dispatch_ops_reply;
  default:
    return flowmq_dispatch_ops_scan;
  }
}

/*
 * Mirrors the current flowmq_socket_try_send() classification order:
 * IDENTITY first-part handling, generic multipart handling, FANOUT, then
 * REP/pinned/ordinary peer selection.
 */
static inline flowmq_dispatch_action_t
flowmq_dispatch_direct(const flowmq_dispatch_case_t *c) {
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !c->sending_multipart)
    return FLOWMQ_DISPATCH_IDENTITY;
  if (c->sending_multipart || c->sndmore)
    return FLOWMQ_DISPATCH_MULTIPART;
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT)
    return FLOWMQ_DISPATCH_FANOUT;
  if (c->desc->fsm_class == FLOWMQ_PATTERN_FSM_REP &&
      !c->send_peer_active)
    return FLOWMQ_DISPATCH_REPLY_PEER;
  if (c->send_peer_active)
    return FLOWMQ_DISPATCH_PINNED;
  return FLOWMQ_DISPATCH_SCAN;
}

/*
 * Candidate B: use the already-canonical routing_class as the dispatch key
 * after the two order-sensitive multipart checks.
 */
/*
 * Candidate B0: stay with direct branches, but source reply-peer routing from
 * routing_class instead of loading the independent transaction fsm_class.
 * This is the narrow semantic cleanup suggested by the steady REP result.
 */
static inline flowmq_dispatch_action_t
flowmq_dispatch_direct_route_reply(const flowmq_dispatch_case_t *c) {
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !c->sending_multipart)
    return FLOWMQ_DISPATCH_IDENTITY;
  if (c->sending_multipart || c->sndmore)
    return FLOWMQ_DISPATCH_MULTIPART;
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT)
    return FLOWMQ_DISPATCH_FANOUT;
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_REPLY_PEER &&
      !c->send_peer_active)
    return FLOWMQ_DISPATCH_REPLY_PEER;
  if (c->send_peer_active)
    return FLOWMQ_DISPATCH_PINNED;
  return FLOWMQ_DISPATCH_SCAN;
}

static inline flowmq_dispatch_action_t
flowmq_dispatch_switch(const flowmq_dispatch_case_t *c) {
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !c->sending_multipart)
    return FLOWMQ_DISPATCH_IDENTITY;
  if (c->sending_multipart || c->sndmore)
    return FLOWMQ_DISPATCH_MULTIPART;

  switch ((flowmq_pattern_route_class_t)c->desc->routing_class) {
  case FLOWMQ_PATTERN_ROUTE_FANOUT:
    return FLOWMQ_DISPATCH_FANOUT;
  case FLOWMQ_PATTERN_ROUTE_REPLY_PEER:
    return c->send_peer_active ? FLOWMQ_DISPATCH_PINNED
                               : FLOWMQ_DISPATCH_REPLY_PEER;
  default:
    return c->send_peer_active ? FLOWMQ_DISPATCH_PINNED
                               : FLOWMQ_DISPATCH_SCAN;
  }
}

/*
 * Candidate C: the per-socket ops pointer is assumed to be cached at socket
 * construction. The benchmark therefore measures the indirect call itself,
 * not a table lookup on every message.
 */
static inline flowmq_dispatch_action_t
flowmq_dispatch_cached_ops(const flowmq_dispatch_case_t *c) {
  if (c->desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !c->sending_multipart)
    return FLOWMQ_DISPATCH_IDENTITY;
  if (c->sending_multipart || c->sndmore)
    return FLOWMQ_DISPATCH_MULTIPART;
  return c->ops((int)c->send_peer_active);
}

static void flowmq_dispatch_init_cases_for(
    flowmq_protocol_pattern_t fixed_pattern, int mixed_patterns) {
  uint32_t state = UINT32_C(0x9e3779b9);

  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i) {
    flowmq_protocol_pattern_t pattern = fixed_pattern;
    const flowmq_pattern_desc_t *desc;

    state = state * UINT32_C(1664525) + UINT32_C(1013904223);
    if (mixed_patterns) {
      pattern = (flowmq_protocol_pattern_t)(
          FLOWMQ_PROTOCOL_PUB +
          (state % (FLOWMQ_PROTOCOL_XSUB - FLOWMQ_PROTOCOL_PUB + 1u)));
    }
    desc = flowmq_pattern_descriptor(pattern);

    FLOWMQ_DISPATCH_CASE_DATA[i].desc = desc;
    FLOWMQ_DISPATCH_CASE_DATA[i].ops = flowmq_dispatch_ops_for(desc);
    FLOWMQ_DISPATCH_CASE_DATA[i].sending_multipart =
        (unsigned)((state >> 8) & 1u);
    FLOWMQ_DISPATCH_CASE_DATA[i].sndmore =
        (unsigned)((state >> 13) & 1u);
    FLOWMQ_DISPATCH_CASE_DATA[i].send_peer_active =
        (unsigned)((state >> 21) & 1u);

    /* Keep order-sensitive fixed-pattern cases close to reachable runtime
     * states. ROUTER pins a peer only after the routing-id first part, and REP
     * pins only while a multipart reply is in progress. */
    if (!mixed_patterns && pattern == FLOWMQ_PROTOCOL_ROUTER)
      FLOWMQ_DISPATCH_CASE_DATA[i].send_peer_active =
          FLOWMQ_DISPATCH_CASE_DATA[i].sending_multipart;
    if (!mixed_patterns && pattern == FLOWMQ_PROTOCOL_REP &&
        !FLOWMQ_DISPATCH_CASE_DATA[i].sending_multipart)
      FLOWMQ_DISPATCH_CASE_DATA[i].send_peer_active = 0u;
  }
}

static void flowmq_dispatch_init_mixed_cases(void) {
  flowmq_dispatch_init_cases_for(FLOWMQ_PROTOCOL_PAIR, 1);
}

static uint64_t flowmq_dispatch_run_direct(void) {
  uint64_t result = 0u;
  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i)
    result += (uint64_t)flowmq_dispatch_direct(&FLOWMQ_DISPATCH_CASE_DATA[i]);
  return result;
}

static uint64_t flowmq_dispatch_run_direct_route_reply(void) {
  uint64_t result = 0u;
  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i)
    result += (uint64_t)flowmq_dispatch_direct_route_reply(
        &FLOWMQ_DISPATCH_CASE_DATA[i]);
  return result;
}

static uint64_t flowmq_dispatch_run_switch(void) {
  uint64_t result = 0u;
  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i)
    result += (uint64_t)flowmq_dispatch_switch(&FLOWMQ_DISPATCH_CASE_DATA[i]);
  return result;
}

static uint64_t flowmq_dispatch_run_cached_ops(void) {
  uint64_t result = 0u;
  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i)
    result +=
        (uint64_t)flowmq_dispatch_cached_ops(&FLOWMQ_DISPATCH_CASE_DATA[i]);
  return result;
}

static void flowmq_dispatch_check_equivalence(void) {
  uint64_t direct_result = flowmq_dispatch_run_direct();
  uint64_t route_reply_result = flowmq_dispatch_run_direct_route_reply();
  uint64_t switch_result = flowmq_dispatch_run_switch();
  uint64_t ops_result = flowmq_dispatch_run_cached_ops();

  for (size_t i = 0u; i < FLOWMQ_DISPATCH_CASES; ++i) {
    flowmq_dispatch_action_t direct =
        flowmq_dispatch_direct(&FLOWMQ_DISPATCH_CASE_DATA[i]);
    check_equal(
        flowmq_dispatch_direct_route_reply(&FLOWMQ_DISPATCH_CASE_DATA[i]),
        direct);
    check_equal(flowmq_dispatch_switch(&FLOWMQ_DISPATCH_CASE_DATA[i]),
                direct);
    check_equal(flowmq_dispatch_cached_ops(&FLOWMQ_DISPATCH_CASE_DATA[i]),
                direct);
  }
  check_equal(route_reply_result, direct_result);
  check_equal(switch_result, direct_result);
  check_equal(ops_result, direct_result);
}

#define FLOWMQ_DISPATCH_BENCH_SET(prefix, samples_)                               \
  benchmark_ops(prefix " direct", samples_, FLOWMQ_DISPATCH_CASES) {              \
    FLOWMQ_DISPATCH_SINK = flowmq_dispatch_run_direct();                          \
  }                                                                               \
  benchmark_ops(prefix " route-reply-direct", samples_, FLOWMQ_DISPATCH_CASES) {  \
    FLOWMQ_DISPATCH_SINK = flowmq_dispatch_run_direct_route_reply();              \
  }                                                                               \
  benchmark_ops(prefix " switch", samples_, FLOWMQ_DISPATCH_CASES) {              \
    FLOWMQ_DISPATCH_SINK = flowmq_dispatch_run_switch();                          \
  }                                                                               \
  benchmark_ops(prefix " cached-ops", samples_, FLOWMQ_DISPATCH_CASES) {          \
    FLOWMQ_DISPATCH_SINK = flowmq_dispatch_run_cached_ops();                      \
  }

spec("FlowMQ dispatch policy candidates") {
  bench("mixed-pattern stress and per-socket steady state") {
    const size_t steady_samples = FLOWMQ_DISPATCH_SAMPLES / 2u;
    uint64_t expected;

    flowmq_dispatch_init_mixed_cases();
    flowmq_dispatch_check_equivalence();
    expected = flowmq_dispatch_run_direct();

    FLOWMQ_DISPATCH_BENCH_SET("mixed", FLOWMQ_DISPATCH_SAMPLES)
    check_equal(FLOWMQ_DISPATCH_SINK, expected);

    flowmq_dispatch_init_cases_for(FLOWMQ_PROTOCOL_PUB, 0);
    flowmq_dispatch_check_equivalence();
    expected = flowmq_dispatch_run_direct();
    FLOWMQ_DISPATCH_BENCH_SET("PUB steady", steady_samples)
    check_equal(FLOWMQ_DISPATCH_SINK, expected);

    flowmq_dispatch_init_cases_for(FLOWMQ_PROTOCOL_PUSH, 0);
    flowmq_dispatch_check_equivalence();
    expected = flowmq_dispatch_run_direct();
    FLOWMQ_DISPATCH_BENCH_SET("PUSH steady", steady_samples)
    check_equal(FLOWMQ_DISPATCH_SINK, expected);

    flowmq_dispatch_init_cases_for(FLOWMQ_PROTOCOL_ROUTER, 0);
    flowmq_dispatch_check_equivalence();
    expected = flowmq_dispatch_run_direct();
    FLOWMQ_DISPATCH_BENCH_SET("ROUTER steady", steady_samples)
    check_equal(FLOWMQ_DISPATCH_SINK, expected);

    flowmq_dispatch_init_cases_for(FLOWMQ_PROTOCOL_REP, 0);
    flowmq_dispatch_check_equivalence();
    expected = flowmq_dispatch_run_direct();
    FLOWMQ_DISPATCH_BENCH_SET("REP steady", steady_samples)
    check_equal(FLOWMQ_DISPATCH_SINK, expected);
  }
}
