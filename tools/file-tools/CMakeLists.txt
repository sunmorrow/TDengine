add_executable(generate_current "main.c")
target_link_libraries(generate_current PRIVATE vnode)
target_include_directories(generate_current PRIVATE "${TD_SOURCE_DIR}/source/dnode/vnode/src/inc")