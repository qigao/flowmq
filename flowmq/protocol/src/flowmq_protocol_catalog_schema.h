#ifndef FLOWMQ_PROTOCOL_CATALOG_SCHEMA_H
#define FLOWMQ_PROTOCOL_CATALOG_SCHEMA_H

#include "flowmq_protocol_catalog.h"

#include <cmeta/pp.h>

/* One compile-time schema is the implementation source for the immutable catalog. */
#define FLOWMQ_PROTOCOL_CATALOG_SCHEMA(M)                                                          \
  Schema(M,                                                                                        \
         (FLOWMQ_PROTOCOL_FAMILY_FMQ, "FMQ", FLOWMQ_PROTOCOL_FMQ_VERSION,                          \
          FLOWMQ_PROTOCOL_LAYER_TRANSPORT,                                                         \
          FLOWMQ_PROTOCOL_CAPABILITY_NEGOTIATION | FLOWMQ_PROTOCOL_CAPABILITY_FLOW_CREDIT |        \
              FLOWMQ_PROTOCOL_CAPABILITY_MULTIPART,                                                \
          'T', 'F', 'M', 'Q', 4u),                                                                 \
         (FLOWMQ_PROTOCOL_FAMILY_FMS, "FMS", FLOWMQ_PROTOCOL_FMS_VERSION,                          \
          FLOWMQ_PROTOCOL_LAYER_SECURITY, FLOWMQ_PROTOCOL_CAPABILITY_SECURITY_ENVELOPE, 'F', 'M',  \
          'S', ('0' + FLOWMQ_PROTOCOL_FMS_VERSION), 4u),                                           \
         (FLOWMQ_PROTOCOL_FAMILY_FES, "FES", FLOWMQ_PROTOCOL_FES_VERSION,                          \
          FLOWMQ_PROTOCOL_LAYER_APPLICATION, FLOWMQ_PROTOCOL_CAPABILITY_APPLICATION_ENVELOPE, 'F', \
          'E', 'S', ('0' + FLOWMQ_PROTOCOL_FES_VERSION), 4u),                                      \
         (FLOWMQ_PROTOCOL_FAMILY_FMP, "FMP", FLOWMQ_PROTOCOL_FMP_VERSION,                          \
          FLOWMQ_PROTOCOL_LAYER_APPLICATION_SCHEMA, FLOWMQ_PROTOCOL_CAPABILITY_TYPED_SCHEMA, 0, 0, \
          0, 0, 0u))

#endif /* FLOWMQ_PROTOCOL_CATALOG_SCHEMA_H */
