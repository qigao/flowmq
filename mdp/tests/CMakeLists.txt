cmake_add_test(test_flowmq_mdp
  SOURCES test_mdp.c
  LIBS FlowMQ::MDP Salts::TinyTest
  FOLDER "mdp/tests")
set_tests_properties(test_flowmq_mdp PROPERTIES LABELS "flowmq;flowmq-mdp"
  TIMEOUT 60 ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
cmake_add_test(test_flowmq_mdp_fanout
  SOURCES test_fanout.c LIBS FlowMQ::MDP Salts::TinyTest FOLDER "mdp/tests")
set_tests_properties(test_flowmq_mdp_fanout PROPERTIES LABELS "flowmq;flowmq-mdp"
  TIMEOUT 60 ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
