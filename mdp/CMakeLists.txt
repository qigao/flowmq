file(GLOB mdp_sources CONFIGURE_DEPENDS src/*.c)
source_group("Source" FILES ${mdp_sources})
add_library(flowmq_mdp STATIC ${mdp_sources})
cmake_config_target(flowmq_mdp ALIAS FlowMQ::MDP EXPORT_NAME MDP FOLDER flowmq)
target_include_directories(
        flowmq_mdp PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                          $<INSTALL_INTERFACE:include>
)
target_link_libraries(flowmq_mdp PUBLIC FlowMQ::FlowMQ)
install(
        TARGETS flowmq_mdp
        EXPORT FlowMQTargets
        ARCHIVE DESTINATION lib COMPONENT FlowMQDevelopment
)
install(
        FILES include/flowmq_mdp.h include/flowmq_mdp_fanout.h
        DESTINATION include
        COMPONENT FlowMQDevelopment
)
if(BUILD_TESTS)
        add_subdirectory(tests)
endif()
