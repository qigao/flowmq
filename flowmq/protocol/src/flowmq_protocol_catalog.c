#include "flowmq_protocol_catalog.h"
#include "flowmq_protocol_catalog_schema.h"

#define FLOWMQ_PROTOCOL_CATALOG_ROW(family_value, name_value, version_value, layer_value,          \
                                    capabilities_value, m0, m1, m2, m3, magic_size_value)          \
  {family_value,                                                                                   \
   name_value,                                                                                     \
   version_value,                                                                                  \
   layer_value,                                                                                    \
   capabilities_value,                                                                             \
   {(unsigned char)(m0), (unsigned char)(m1), (unsigned char)(m2), (unsigned char)(m3)},           \
   magic_size_value},

static const flowmq_protocol_descriptor_t FLOWMQ_PROTOCOL_CATALOG[] = {
    Replay(FLOWMQ_PROTOCOL_CATALOG_SCHEMA, FLOWMQ_PROTOCOL_CATALOG_ROW)};

#undef FLOWMQ_PROTOCOL_CATALOG_ROW

typedef char flowmq_protocol_catalog_count_matches_family_count
    [sizeof(FLOWMQ_PROTOCOL_CATALOG) / sizeof(FLOWMQ_PROTOCOL_CATALOG[0]) ==
             FLOWMQ_PROTOCOL_FAMILY_COUNT
         ? 1
         : -1];

size_t flowmq_protocol_catalog_count(void) {
  return sizeof(FLOWMQ_PROTOCOL_CATALOG) / sizeof(FLOWMQ_PROTOCOL_CATALOG[0]);
}

const flowmq_protocol_descriptor_t *flowmq_protocol_catalog_at(size_t index) {
  return index < flowmq_protocol_catalog_count() ? &FLOWMQ_PROTOCOL_CATALOG[index] : NULL;
}

const flowmq_protocol_descriptor_t *flowmq_protocol_catalog_get(flowmq_protocol_family_t family) {
  size_t index;
  for (index = 0u; index < flowmq_protocol_catalog_count(); ++index) {
    if (FLOWMQ_PROTOCOL_CATALOG[index].family == family) {
      return &FLOWMQ_PROTOCOL_CATALOG[index];
    }
  }
  return NULL;
}
