#ifndef TURBO_FLOW_FMQ_BROKER_PROTOCOL_H
#define TURBO_FLOW_FMQ_BROKER_PROTOCOL_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION 1u
#define TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX 127u
#define TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE 24u
#define TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE                                                    \
  (TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + 2u * TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX)

/**
 * Durable, network-safe return address for an inter-broker request.
 *
 * `origin_broker_id` selects the broker that owns the client connection;
 * `client_id` is stable within that broker; `request_id` is monotonic or unique
 * within that client identity. No process-local protocol route is represented.
 */
typedef struct turbo_flow_fmq_broker_logical_address_s {
  size_t size;
  uint32_t version;
  char origin_broker_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  char client_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  uint64_t request_id;
} turbo_flow_fmq_broker_logical_address_t;

#define TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT                                                 \
  {sizeof(turbo_flow_fmq_broker_logical_address_t),                                                \
   TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION,                                                         \
   {0},                                                                                            \
   {0},                                                                                            \
   0u}

/**
 * Validate a logical address without changing state.
 *
 * @param address Address initialized with TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT.
 * @return TURBO_OK, or TURBO_EINVAL for an invalid version, size, ID, or request ID.
 */
CXX_C_API int turbo_flow_fmq_broker_logical_address_validate(
    const turbo_flow_fmq_broker_logical_address_t *address);

/**
 * Encode one logical address using the versioned TFBR binary envelope.
 *
 * @param address Valid logical address to encode.
 * @param out Destination buffer. It may be NULL when querying the required size.
 * @param capacity Size of `out` in bytes.
 * @param out_len Receives the exact encoded size.
 * @return TURBO_OK, TURBO_EINVAL for invalid arguments/address, or TURBO_ENOSPC when
 *         `capacity` is too small.
 * On TURBO_ENOSPC, `out_len` contains the required capacity and `out` is unchanged.
 *
 * Example:
 * @code
 * turbo_flow_fmq_broker_logical_address_t address =
 *     TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
 * uint8_t bytes[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
 * size_t bytes_len = 0;
 * memcpy(address.origin_broker_id, "broker-a", sizeof("broker-a"));
 * memcpy(address.client_id, "client-7", sizeof("client-7"));
 * address.request_id = 42;
 * int rc = turbo_flow_fmq_broker_logical_address_encode(
 *     &address, bytes, sizeof(bytes), &bytes_len);
 * @endcode
 */
CXX_C_API int
turbo_flow_fmq_broker_logical_address_encode(const turbo_flow_fmq_broker_logical_address_t *address,
                                             uint8_t *out, size_t capacity, size_t *out_len);

/**
 * Decode exactly one TFBR envelope.
 *
 * @param data Encoded envelope bytes.
 * @param data_len Exact envelope length; trailing bytes are rejected.
 * @param out Output initialized with TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT. It is
 *            unchanged when decoding fails.
 * @return TURBO_OK, TURBO_EINVAL for invalid arguments/output ABI size, or TURBO_EPROTO
 *         for a malformed or unsupported envelope.
 */
CXX_C_API int
turbo_flow_fmq_broker_logical_address_decode(const uint8_t *data, size_t data_len,
                                             turbo_flow_fmq_broker_logical_address_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_BROKER_PROTOCOL_H */
