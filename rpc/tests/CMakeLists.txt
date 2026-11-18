cmake_add_test(test_flowmq_rpc
  SOURCES test_rpc.c
  LIBS FlowMQ::RPC Salts::TinyTest
  FOLDER "rpc/tests")
set_tests_properties(test_flowmq_rpc PROPERTIES LABELS "flowmq;flowmq-rpc"
  TIMEOUT 60 ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")

cmake_add_test(test_flowmq_rpc_scheduler
  SOURCES test_scheduler.c
  LIBS FlowMQ::RPC Salts::TinyTest
  FOLDER "rpc/tests")
set_tests_properties(test_flowmq_rpc_scheduler PROPERTIES LABELS "flowmq;flowmq-rpc"
  TIMEOUT 60 ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
