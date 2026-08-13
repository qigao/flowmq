add_executable(lemon lemon.c)
target_compile_features(lemon PRIVATE c_std_11)

set_target_properties(lemon PROPERTIES FOLDER tools)
