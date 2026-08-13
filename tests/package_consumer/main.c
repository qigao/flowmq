#include "flowmq.h"

int main(void) {
  flowmq_connect_endpoint_config_t config;
  flowmq_router_endpoint_config_t router;
  ProviderCommandV1_t command;
  flowmq_connect_endpoint_config_init(&config);
  flowmq_router_endpoint_config_init(&router);
  ProviderCommandV1_init(&command);
  ProviderCommandV1_clear(&command);
  return flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_PUSH, FLOWMQ_PROTOCOL_PULL) &&
                 config.size == sizeof(config) && config.transport == FLOWMQ_TRANSPORT_TCP &&
                 router.size == sizeof(router) &&
                 router.max_connections == FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_CONNECTIONS &&
                 FLOWMQ_CONNECT_ENDPOINT_API_VERSION == 2u &&
                 FLOWMQ_ROUTER_ENDPOINT_API_VERSION == 3u &&
                 FLOWMQ_MEDIA_PROVIDER_API_VERSION == 3u &&
                 ProviderCommandV1_message_kind_OFFSET == 4u &&
                 ProviderMessageKind_Receipt == 2u &&
                 ProviderMessageKind_CompletionAck == 10u &&
                 ProviderReceiptDisposition_DurableAccepted == 1u
             ? 0
             : 1;
}
