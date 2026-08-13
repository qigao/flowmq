# The independent build is rooted at ../CMakeLists.txt and enters protocol/
# and runtime/ directly. Fail fast for obsolete TurboFlow integrations.
message(FATAL_ERROR
        "Legacy TurboFlow::FMQ integration is not part of FlowMQ. Configure the repository root and link FlowMQ::FlowMQ.")
