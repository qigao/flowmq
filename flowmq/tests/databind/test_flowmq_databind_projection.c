#include "flowmq_contract_channel.flowmq.h"
#include "flowmq_contract_service.flowmq.h"

#include "flowmq_socket.h"
#include "tinytest.h"

#include <data_bind_flowmq_plan.h>

#include <string.h>

spec("FlowMQ generated DataBind projection boundary") {
  it("consumes one canonical ChannelPlan without a FlowMQ-private schema model") {
    DataBindNativeTypeBinding binding = {0};
    DataBindError error = DATA_BIND_ERROR_INIT;
    const DataBindFlowMQChannelPlan *plan =
        &databind_flowmq_contract_channel_flowmq_channel_plan;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *publisher = NULL;
    flowmq_socket_t *subscriber = NULL;

    check_not_null(ctx);
    check_equal(plan->size, sizeof(*plan));
    check_equal(plan->abi_version,
                (uint32_t)DATA_BIND_FLOWMQ_CHANNEL_PLAN_ABI_VERSION);
    check_not_null(plan->channel_name);
    check_equal(strcmp(plan->channel_name, "FlowMQQual.Telemetry"), 0);
    check_not_null(plan->message_type);
    check_equal(strcmp(plan->message_type, "TelemetryEvent"), 0);
    check_equal(plan->format, DATA_BIND_FORMAT_BINARY);
    check_equal(plan->pattern, DATA_BIND_FLOWMQ_CHANNEL_PUB_SUB);
    check_equal(plan->max_payload_bytes, (size_t)65536u);
    check_not_null(plan->native_binding);
    check_equal(plan->native_binding(&binding, &error), DATA_BIND_OK);
    check_not_null(binding.idl_type_name);
    check_equal(strcmp(binding.idl_type_name, "TelemetryEvent"), 0);

    publisher = flowmq_socket(ctx, FLOWMQ_PUB);
    subscriber = flowmq_socket(ctx, FLOWMQ_SUB);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_equal(flowmq_close(publisher), SALTS_OK);
    check_equal(flowmq_close(subscriber), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("consumes one canonical ServicePlan without moving Service semantics into FlowMQ") {
    DataBindNativeTypeBinding request = {0};
    DataBindNativeTypeBinding response = {0};
    DataBindError error = DATA_BIND_ERROR_INIT;
    const DataBindFlowMQServicePlan *plan =
        &databind_flowmq_contract_service_flowmq_service_plan;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *client = NULL;
    flowmq_socket_t *server = NULL;

    check_not_null(ctx);
    check_equal(plan->size, sizeof(*plan));
    check_equal(plan->abi_version,
                (uint32_t)DATA_BIND_FLOWMQ_SERVICE_PLAN_ABI_VERSION);
    check_not_null(plan->service_name);
    check_equal(strcmp(plan->service_name, "FlowMQQual.Calc"), 0);
    check_not_null(plan->operation_name);
    check_equal(strcmp(plan->operation_name, "FlowMQQual.Calc.Add"), 0);
    check_equal(plan->pattern, DATA_BIND_FLOWMQ_SERVICE_REQ_REP);
    check_equal(plan->ingress_format, DATA_BIND_FORMAT_JSON);
    check_equal(plan->egress_format, DATA_BIND_FORMAT_JSON);
    check_equal(plan->max_payload_bytes, (size_t)65536u);
    check_not_null(plan->request_native_binding);
    check_not_null(plan->response_native_binding);
    check_equal(plan->request_native_binding(&request, &error), DATA_BIND_OK);
    check_equal(plan->response_native_binding(&response, &error), DATA_BIND_OK);
    check_not_null(request.idl_type_name);
    check_not_null(response.idl_type_name);
    check_equal(strcmp(request.idl_type_name, "AddRequest"), 0);
    check_equal(strcmp(response.idl_type_name, "AddResponse"), 0);

    client = flowmq_socket(ctx, FLOWMQ_REQ);
    server = flowmq_socket(ctx, FLOWMQ_REP);
    check_not_null(client);
    check_not_null(server);
    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
