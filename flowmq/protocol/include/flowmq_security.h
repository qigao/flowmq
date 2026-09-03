#ifndef FLOWMQ_SECURITY_H
#define FLOWMQ_SECURITY_H

#include "flowmq_export.h"
#include "flowmq_protocol_catalog.h"

#include "turbo_str.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_SECURITY_HEADER_SIZE 12u
#define FLOWMQ_SECURITY_MAX_IDENTITY_SIZE 255u
#define FLOWMQ_SECURITY_MAX_AUTH_METHOD_SIZE 63u
#define FLOWMQ_SECURITY_MAX_AUTH_SECRET_SIZE 4096u
#define FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE 32u

typedef enum flowmq_security_mode_e {
  FLOWMQ_SECURITY_NONE = 0,
  FLOWMQ_SECURITY_AUTH = 1,
  FLOWMQ_SECURITY_ACCEPTED = 2
} flowmq_security_mode_t;

/** Borrowed views for one decoded FMS/3 HELLO security envelope. */
typedef struct flowmq_security_s {
  flowmq_security_mode_t mode;
  vstr identity;
  vstr method;
  vstr secret;
  vstr channel_binding;
} flowmq_security_t;

/**
 * Encode one FMS/3 envelope. NONE produces an owned empty payload.
 *
 * @param security Borrowed validated input fields.
 * @param payload Output initialized to NULL; caller releases success output with tstr_free().
 * @return TURBO_OK, TURBO_EINVAL, TURBO_EPROTO, TURBO_EMSGSIZE, or TURBO_ENOMEM.
 */
FLOWMQ_C_API int flowmq_security_encode(const flowmq_security_t *security, tstr *payload);
/**
 * Strictly decode one complete FMS/3 envelope into views borrowed from `payload`.
 *
 * @param payload Encoded bytes; an empty view represents FLOWMQ_SECURITY_NONE.
 * @param security Output reset to zero before decoding and on malformed input.
 * @return TURBO_OK, TURBO_EINVAL, TURBO_EPROTO, TURBO_EMSGSIZE, or TURBO_ERANGE.
 */
FLOWMQ_C_API int flowmq_security_decode(vstr payload, flowmq_security_t *security);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SECURITY_H */
