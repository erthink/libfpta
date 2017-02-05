include_directories(${UT_INCLUDE_DIRECTORIES})

add_executable(fpta_c_mode c_mode.c)
target_link_libraries(fpta_c_mode fpta)

add_ut(fpta0_corny TIMEOUT 1 SOURCE 0corny.cxx LIBRARY fpta)
add_ut(fpta1_open TIMEOUT 1 SOURCE 1open.cxx LIBRARY fpta)
add_ut(fpta2_schema TIMEOUT 1 SOURCE 2schema.cxx LIBRARY fpta)
add_ut(fpta3_smoke TIMEOUT 5 SOURCE 3smoke.cxx keygen.cxx LIBRARY fpta)
add_ut(fpta4_data TIMEOUT 1 SOURCE 4data.cxx LIBRARY fpta)

add_ut(fpta5_key TIMEOUT 5 SOURCE 5key.cxx keygen.hpp LIBRARY fpta)
add_ut(fpta6_index_primary TIMEOUT 60 SOURCE 6index_primary.cxx keygen.cxx LIBRARY fpta)
add_ut(fpta6_index_secondary TIMEOUT 300 SOURCE 6index_secondary.cxx keygen.cxx LIBRARY fpta)
add_ut(fpta7_cursor_primary TIMEOUT 120 SOURCE 7cursor_primary.cxx keygen.cxx tools.hpp LIBRARY fpta)
add_ut(fpta7_cursor_secondary TIMEOUT 120 SOURCE 7cursor_secondary.cxx keygen.cxx tools.hpp LIBRARY fpta)
