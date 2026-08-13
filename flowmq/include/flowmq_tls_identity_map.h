#ifndef FLOWMQ_TLS_IDENTITY_MAP_H
#define FLOWMQ_TLS_IDENTITY_MAP_H

#include "flowmq_protocol.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_TLS_IDENTITY_MAP_API_VERSION 1u
#define FLOWMQ_TLS_IDENTITY_MAP_MAX_BINDINGS 1024u
#define FLOWMQ_TLS_IDENTITY_MAP_DEFAULT_MAX_TOTAL_STRING_BYTES \
  (64u * (72u + FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u))
#define FLOWMQ_TLS_IDENTITY_MAP_MAX_TOTAL_STRING_BYTES \
  (FLOWMQ_TLS_IDENTITY_MAP_MAX_BINDINGS * \
   (72u + FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u))

typedef struct flowmq_tls_identity_map_s flowmq_tls_identity_map_t;

typedef struct flowmq_tls_identity_binding_s {
  size_t size;
  /** Canonical sha256: followed by 64 lowercase hexadecimal characters. */
  const char *certificate_sha256;
  /** NUL-terminated FlowMQ HELLO identity. */
  const char *hello_identity;
} flowmq_tls_identity_binding_t;

typedef struct flowmq_tls_identity_map_config_s {
  size_t size;
  const flowmq_tls_identity_binding_t *bindings;
  size_t binding_count;
  /** Zero selects FLOWMQ_TLS_IDENTITY_MAP_DEFAULT_MAX_TOTAL_STRING_BYTES. */
  size_t max_total_string_bytes;
  uint64_t policy_generation;
} flowmq_tls_identity_map_config_t;

#define FLOWMQ_TLS_IDENTITY_BINDING_INIT \
  { sizeof(flowmq_tls_identity_binding_t), NULL, NULL }
#define FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT \
  { sizeof(flowmq_tls_identity_map_config_t), NULL, 0u, 0u, 0u }

/**
 * Create an immutable, bounded exact-match policy and copy every input string.
 * One certificate may map to only one identity; multiple certificates may map
 * to the same identity for explicit rotation overlap.
 *
 * @return TURBO_OK, TURBO_EINVAL, TURBO_ERANGE, or TURBO_ENOMEM.
 */
CXX_C_API int flowmq_tls_identity_map_create(
    const flowmq_tls_identity_map_config_t *config,
    flowmq_tls_identity_map_t **out);

/**
 * Verify one exact (certificate fingerprint, HELLO identity) tuple.
 * All inputs are borrowed for the call. The immutable map may be shared by
 * callers only while its owner guarantees destroy cannot run concurrently.
 *
 * @return TURBO_OK, TURBO_EINVAL for malformed input, or TURBO_EPERM when the
 * tuple is not authorized.
 */
CXX_C_API int flowmq_tls_identity_map_verify(
    void *map, const char *certificate_sha256, tstr_v claimed_identity);

/** Return the immutable policy generation, or zero for NULL. */
CXX_C_API uint64_t flowmq_tls_identity_map_generation(
    const flowmq_tls_identity_map_t *map);

/** Destroy the map after every endpoint callback using it is quiescent. */
CXX_C_API void flowmq_tls_identity_map_destroy(flowmq_tls_identity_map_t *map);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_TLS_IDENTITY_MAP_H */
