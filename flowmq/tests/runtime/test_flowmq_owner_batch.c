#include "flowmq_owner_batch.h"

#include "cmeta_error.h"
#include "tinytest.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

typedef struct owner_batch_fixture_s {
  size_t visits[4][3];
  size_t error_id;
  size_t error_slot;
  size_t unclaimed_id;
  int error_status;
} owner_batch_fixture;

static int owner_batch_fake_route(
    void *user, size_t slot, const native_io_completion *completion,
    bool *consumed, size_t *events) {
  owner_batch_fixture *fixture = (owner_batch_fixture *)user;
  const size_t id = completion->bytes;
  if (fixture == NULL || slot >= 3u || id == 0u || id >= 4u)
    return SALTS_EINVAL;
  ++fixture->visits[id][slot];
  *consumed = false;
  if (events != NULL) *events = 0u;
  if (id == fixture->error_id && slot == fixture->error_slot) {
    /* Model a router that consumed a real completion before reporting an
     * error. Another socket MUST NOT receive the ambiguous event. */
    *consumed = true;
    return fixture->error_status;
  }
  if (id != fixture->unclaimed_id && slot == 1u) {
    *consumed = true;
    if (events != NULL) *events = 1u;
  }
  return SALTS_OK;
}

spec("FlowMQ observed Owner completion-batch settlement") {
  it("settles later completions after a first partial-consumption error") {
    native_io_completion observed[3] = {
        {.bytes = 1u}, {.bytes = 2u}, {.bytes = 3u}};
    owner_batch_fixture fixture = {0};
    fixture.error_id = 1u;
    fixture.error_slot = 0u;
    fixture.error_status = SALTS_EPERM;

    check_equal(flowmq_owner_batch_route(
                    observed, 3u, 3u, owner_batch_fake_route, &fixture),
                SALTS_EPERM);
    check_equal(fixture.visits[1][0], 1u);
    check_equal(fixture.visits[1][1], 0u);
    check_equal(fixture.visits[1][2], 0u);
    check_equal(fixture.visits[2][0], 1u);
    check_equal(fixture.visits[2][1], 1u);
    check_equal(fixture.visits[2][2], 0u);
    check_equal(fixture.visits[3][0], 1u);
    check_equal(fixture.visits[3][1], 1u);
    check_equal(fixture.visits[3][2], 0u);
  }

  it("preserves the first error after a middle router fails and a later event is unclaimed") {
    native_io_completion observed[3] = {
        {.bytes = 1u}, {.bytes = 2u}, {.bytes = 3u}};
    owner_batch_fixture fixture = {0};
    fixture.error_id = 2u;
    fixture.error_slot = 1u;
    fixture.error_status = SALTS_EPERM;
    fixture.unclaimed_id = 3u;

    check_equal(flowmq_owner_batch_route(
                    observed, 3u, 3u, owner_batch_fake_route, &fixture),
                SALTS_EPERM);
    check_equal(fixture.visits[1][1], 1u);
    check_equal(fixture.visits[2][0], 1u);
    check_equal(fixture.visits[2][1], 1u);
    check_equal(fixture.visits[2][2], 0u);
    /* Genuine unclaimed later event still visits all eligible routes. */
    check_equal(fixture.visits[3][0], 1u);
    check_equal(fixture.visits[3][1], 1u);
    check_equal(fixture.visits[3][2], 1u);
  }

  it("does not hide an unclaimed completion and still processes its neighbor") {
    native_io_completion observed[2] = {
        {.bytes = 1u}, {.bytes = 2u}};
    owner_batch_fixture fixture = {0};
    fixture.unclaimed_id = 1u;

    check_equal(flowmq_owner_batch_route(
                    observed, 2u, 3u, owner_batch_fake_route, &fixture),
                SALTS_EPROTO);
    check_equal(fixture.visits[1][2], 1u);
    check_equal(fixture.visits[2][1], 1u);
    check_equal(fixture.visits[2][2], 0u);
  }

  it("rejects malformed routing input and accepts a genuinely empty batch") {
    native_io_completion observed = {.bytes = 1u};
    owner_batch_fixture fixture = {0};
    check_equal(flowmq_owner_batch_route(
                    NULL, 1u, 1u, owner_batch_fake_route, &fixture),
                SALTS_EINVAL);
    check_equal(flowmq_owner_batch_route(
                    &observed, 1u, 0u, owner_batch_fake_route, &fixture),
                SALTS_EINVAL);
    check_equal(flowmq_owner_batch_route(
                    &observed, 1u, 1u, NULL, &fixture),
                SALTS_EINVAL);
    check_equal(flowmq_owner_batch_route(NULL, 0u, 0u, NULL, NULL),
                SALTS_OK);
    check_equal(fixture.visits[1][0], 0u);
  }
}
