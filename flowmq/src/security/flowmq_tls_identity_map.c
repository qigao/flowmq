#include "flowmq_tls_identity_map.h"

#include "salts_error.h"

#include <stdlib.h>
#include <string.h>

enum {
  FLOWMQ_TLS_CERTIFICATE_SHA256_TEXT_SIZE = 71u,
  FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY = 72u
};

typedef struct flowmq_tls_identity_map_entry_s {
  char certificate_sha256[FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY];
  char hello_identity[FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u];
  size_t hello_identity_size;
} flowmq_tls_identity_map_entry_t;

struct flowmq_tls_identity_map_s {
  flowmq_tls_identity_map_entry_t *entries;
  size_t entry_count;
  size_t retained_string_bytes;
  uint64_t policy_generation;
};

static int flowmq_tls_identity_bounded_length(const char *text, size_t maximum,
                                              size_t *out_size) {
  size_t size = 0u;
  if (!text || !out_size) return SALTS_EINVAL;
  while (size <= maximum && text[size] != '\0') ++size;
  if (size > maximum) return SALTS_ERANGE;
  *out_size = size;
  return SALTS_OK;
}

static int flowmq_tls_identity_fingerprint_validate(const char *fingerprint) {
  static const char prefix[] = "sha256:";
  size_t size = 0u;
  int rc = flowmq_tls_identity_bounded_length(
      fingerprint, FLOWMQ_TLS_CERTIFICATE_SHA256_TEXT_SIZE, &size);
  if (rc != SALTS_OK || size != FLOWMQ_TLS_CERTIFICATE_SHA256_TEXT_SIZE ||
      memcmp(fingerprint, prefix, sizeof(prefix) - 1u) != 0)
    return SALTS_EINVAL;
  for (size_t i = sizeof(prefix) - 1u; i < size; ++i) {
    char value = fingerprint[i];
    if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')))
      return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int flowmq_tls_identity_string_validate(const char *identity,
                                               size_t *out_size) {
  int rc = flowmq_tls_identity_bounded_length(
      identity, FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE, out_size);
  if (rc != SALTS_OK || *out_size == 0u) return SALTS_EINVAL;
  return SALTS_OK;
}

int flowmq_tls_identity_map_create(
    const flowmq_tls_identity_map_config_t *config,
    flowmq_tls_identity_map_t **out) {
  flowmq_tls_identity_map_t *map = NULL;
  size_t retained_limit;
  size_t retained = 0u;
  int rc = SALTS_OK;
  if (!config || !out || config->size < sizeof(*config)) return SALTS_EINVAL;
  *out = NULL;
  retained_limit = config->max_total_string_bytes
                       ? config->max_total_string_bytes
                       : FLOWMQ_TLS_IDENTITY_MAP_DEFAULT_MAX_TOTAL_STRING_BYTES;
  if (!config->bindings || config->binding_count == 0u)
    return SALTS_EINVAL;
  if (config->binding_count > FLOWMQ_TLS_IDENTITY_MAP_MAX_BINDINGS ||
      config->binding_count > SIZE_MAX / sizeof(*map->entries) ||
      retained_limit == 0u ||
      retained_limit > FLOWMQ_TLS_IDENTITY_MAP_MAX_TOTAL_STRING_BYTES)
    return SALTS_ERANGE;
  map = (flowmq_tls_identity_map_t *)calloc(1u, sizeof(*map));
  if (!map) return SALTS_ENOMEM;
  map->entries = (flowmq_tls_identity_map_entry_t *)calloc(
      config->binding_count, sizeof(*map->entries));
  if (!map->entries) {
    flowmq_tls_identity_map_destroy(map);
    return SALTS_ENOMEM;
  }
  for (size_t i = 0u; i < config->binding_count; ++i) {
    const flowmq_tls_identity_binding_t *binding = &config->bindings[i];
    flowmq_tls_identity_map_entry_t *entry = &map->entries[i];
    size_t identity_size = 0u;
    size_t tuple_bytes;
    if (binding->size < sizeof(*binding) ||
        flowmq_tls_identity_fingerprint_validate(
            binding->certificate_sha256) != SALTS_OK ||
        flowmq_tls_identity_string_validate(binding->hello_identity,
                                            &identity_size) != SALTS_OK) {
      rc = SALTS_EINVAL;
      goto failed;
    }
    tuple_bytes = FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY + identity_size + 1u;
    if (retained > retained_limit || tuple_bytes > retained_limit - retained) {
      rc = SALTS_ERANGE;
      goto failed;
    }
    for (size_t previous = 0u; previous < i; ++previous) {
      if (memcmp(map->entries[previous].certificate_sha256,
                 binding->certificate_sha256,
                 FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY) == 0) {
        rc = SALTS_EINVAL;
        goto failed;
      }
    }
    memcpy(entry->certificate_sha256, binding->certificate_sha256,
           FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY);
    memcpy(entry->hello_identity, binding->hello_identity, identity_size + 1u);
    entry->hello_identity_size = identity_size;
    retained += tuple_bytes;
  }
  map->entry_count = config->binding_count;
  map->retained_string_bytes = retained;
  map->policy_generation = config->policy_generation;
  *out = map;
  return SALTS_OK;

failed:
  flowmq_tls_identity_map_destroy(map);
  return rc;
}

int flowmq_tls_identity_map_verify(void *map_pointer,
                                   const char *certificate_sha256,
                                   vstr claimed_identity) {
  const flowmq_tls_identity_map_t *map =
      (const flowmq_tls_identity_map_t *)map_pointer;
  if (!map || !claimed_identity.data || claimed_identity.len == 0u ||
      claimed_identity.len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE ||
      flowmq_tls_identity_fingerprint_validate(certificate_sha256) != SALTS_OK)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < map->entry_count; ++i) {
    const flowmq_tls_identity_map_entry_t *entry = &map->entries[i];
    if (memcmp(entry->certificate_sha256, certificate_sha256,
               FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY) == 0 &&
        entry->hello_identity_size == claimed_identity.len &&
        memcmp(entry->hello_identity, claimed_identity.data,
               claimed_identity.len) == 0)
      return SALTS_OK;
  }
  return SALTS_EPERM;
}

uint64_t flowmq_tls_identity_map_generation(
    const flowmq_tls_identity_map_t *map) {
  return map ? map->policy_generation : 0u;
}

void flowmq_tls_identity_map_destroy(flowmq_tls_identity_map_t *map) {
  if (!map) return;
  free(map->entries);
  free(map);
}
