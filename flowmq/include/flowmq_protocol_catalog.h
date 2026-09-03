#ifndef FLOWMQ_PROTOCOL_CATALOG_H
#define FLOWMQ_PROTOCOL_CATALOG_H

#include "flowmq_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_PROTOCOL_CATALOG_API_VERSION 1u
#define FLOWMQ_PROTOCOL_FMQ_VERSION 6u
#define FLOWMQ_PROTOCOL_FMS_VERSION 3u
#define FLOWMQ_PROTOCOL_FES_VERSION 1u
#define FLOWMQ_PROTOCOL_FMP_VERSION 1u

typedef enum flowmq_protocol_family_e {
  FLOWMQ_PROTOCOL_FAMILY_FMQ = 0,
  FLOWMQ_PROTOCOL_FAMILY_FMS,
  FLOWMQ_PROTOCOL_FAMILY_FES,
  FLOWMQ_PROTOCOL_FAMILY_FMP,
  FLOWMQ_PROTOCOL_FAMILY_COUNT
} flowmq_protocol_family_t;

typedef enum flowmq_protocol_layer_e {
  FLOWMQ_PROTOCOL_LAYER_TRANSPORT = 1,
  FLOWMQ_PROTOCOL_LAYER_SECURITY,
  FLOWMQ_PROTOCOL_LAYER_APPLICATION,
  FLOWMQ_PROTOCOL_LAYER_APPLICATION_SCHEMA
} flowmq_protocol_layer_t;

typedef enum flowmq_protocol_capability_e {
  FLOWMQ_PROTOCOL_CAPABILITY_NEGOTIATION = 0x00000001u,
  FLOWMQ_PROTOCOL_CAPABILITY_FLOW_CREDIT = 0x00000002u,
  FLOWMQ_PROTOCOL_CAPABILITY_MULTIPART = 0x00000004u,
  FLOWMQ_PROTOCOL_CAPABILITY_SECURITY_ENVELOPE = 0x00000008u,
  FLOWMQ_PROTOCOL_CAPABILITY_APPLICATION_ENVELOPE = 0x00000010u,
  FLOWMQ_PROTOCOL_CAPABILITY_TYPED_SCHEMA = 0x00000020u
} flowmq_protocol_capability_t;

/** FMQ/6 transport pattern values; application protocols cannot extend this range. */
typedef uint8_t flowmq_protocol_pattern_t;

typedef enum flowmq_protocol_pattern_value_e {
  FLOWMQ_PROTOCOL_PUB = 1,
  FLOWMQ_PROTOCOL_SUB,
  FLOWMQ_PROTOCOL_PUSH,
  FLOWMQ_PROTOCOL_PULL,
  FLOWMQ_PROTOCOL_ROUTER,
  FLOWMQ_PROTOCOL_DEALER,
  FLOWMQ_PROTOCOL_PAIR,
  FLOWMQ_PROTOCOL_REQ,
  FLOWMQ_PROTOCOL_REP,
  FLOWMQ_PROTOCOL_XPUB,
  FLOWMQ_PROTOCOL_XSUB
} flowmq_protocol_pattern_value_t;

/** FMQ/6 frame kinds; values 7..31 are invalid rather than extension slots. */
typedef enum flowmq_protocol_frame_kind_e {
  FLOWMQ_PROTOCOL_FRAME_HELLO = 1,
  FLOWMQ_PROTOCOL_FRAME_DATA,
  FLOWMQ_PROTOCOL_FRAME_PING,
  FLOWMQ_PROTOCOL_FRAME_PONG,
  FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
  FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE,
  FLOWMQ_PROTOCOL_FRAME_SETTINGS = 32,
  FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE = 33
} flowmq_protocol_frame_kind_t;

/** Immutable process-lifetime metadata for one current protocol family. */
typedef struct flowmq_protocol_descriptor_s {
  flowmq_protocol_family_t family;
  const char *name;
  uint32_t version;
  flowmq_protocol_layer_t layer;
  uint32_t capabilities;
  unsigned char magic[4];
  uint8_t magic_size;
} flowmq_protocol_descriptor_t;

/** @return The number of current protocol families in the immutable catalog. */
FLOWMQ_C_API size_t flowmq_protocol_catalog_count(void);
/**
 * @param index Stable catalog index in `[0, flowmq_protocol_catalog_count())`.
 * @return A process-lifetime borrowed descriptor, or NULL when `index` is invalid.
 */
FLOWMQ_C_API const flowmq_protocol_descriptor_t *flowmq_protocol_catalog_at(size_t index);
/**
 * @param family One current protocol family.
 * @return A process-lifetime borrowed descriptor, or NULL when `family` is unknown.
 */
FLOWMQ_C_API const flowmq_protocol_descriptor_t *
flowmq_protocol_catalog_get(flowmq_protocol_family_t family);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_PROTOCOL_CATALOG_H */
