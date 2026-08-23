#include "flowmq_core.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_core") {
  it("exposes the public pattern contracts") {
    check_equal(flowmq_core_pattern_validate(FLOWMQ_PROTOCOL_PUB), TURBO_OK);
    check_equal(flowmq_core_pattern_validate(0u), TURBO_EINVAL);
    check_true(flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_ROUTER, FLOWMQ_PROTOCOL_DEALER));
    check_false(flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_DEALER));
  }
}
