include_directories(${UT_INCLUDE_DIRECTORIES})

if(BUILD_SHARED_LIBS)
  add_definitions("-DGTEST_LINKED_AS_SHARED_LIBRARY=1")
endif()

add_library(testutils STATIC
  keygen.cxx
  keygen.hpp
  fpta_test.h
  tools.hpp
  crutch4windows.cxx)
target_link_libraries(testutils ${UT_LIBRARIES})

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(fpta_small_timeout 3)
  set(fpta3_smoke_timeout 10)
  set(fpta5_key_timeout 30)
  set(fpta6_index_primary_timeout 200)
  set(fpta6_index_secondary_timeout 1500)
  set(fpta7_cursor_primary_timeout 600)
  set(fpta7_cursor_secondary_unique_timeout 3000)
  set(fpta7_cursor_secondary_withdups_timeout 3000)
  set(fpta9_crud_timeout 300)
else()
  set(fpta_small_timeout 1)
  set(fpta3_smoke_timeout 5)
  set(fpta5_key_timeout 5)
  set(fpta6_index_primary_timeout 60)
  set(fpta6_index_secondary_timeout 300)
  set(fpta7_cursor_primary_timeout 120)
  set(fpta7_cursor_secondary_unique_timeout 600)
  set(fpta7_cursor_secondary_withdups_timeout 600)
  set(fpta9_crud_timeout 120)
endif()

add_executable(fpta_c_mode c_mode.c)
target_link_libraries(fpta_c_mode fpta)

add_ut(fpta0_corny TIMEOUT ${fpta_small_timeout} SOURCE 0corny.cxx LIBRARY fpta testutils)
add_ut(fpta1_open TIMEOUT ${fpta_small_timeout} SOURCE 1open.cxx LIBRARY fpta testutils)
add_ut(fpta2_schema TIMEOUT ${fpta_small_timeout} SOURCE 2schema.cxx LIBRARY fpta testutils)
add_ut(fpta3_smoke TIMEOUT ${fpta3_smoke_timeout} SOURCE 3smoke.cxx LIBRARY fpta testutils)
add_ut(fpta4_data TIMEOUT ${fpta_small_timeout} SOURCE 4data.cxx LIBRARY fpta testutils)

add_ut(fpta5_key TIMEOUT ${fpta5_key_timeout} SOURCE 5key.cxx LIBRARY fpta testutils)
add_ut(fpta6_index_primary TIMEOUT ${fpta6_index_primary_timeout} SOURCE 6index_primary.cxx LIBRARY fpta testutils)
add_ut(fpta6_index_secondary TIMEOUT ${fpta6_index_secondary_timeout} SOURCE 6index_secondary.cxx LIBRARY fpta testutils)
add_ut(fpta7_cursor_primary TIMEOUT ${fpta7_cursor_primary_timeout} SOURCE 7cursor_primary.cxx LIBRARY fpta testutils)
add_ut(fpta7_cursor_secondary_unique TIMEOUT ${fpta7_cursor_secondary_unique_timeout} SOURCE 7cursor_secondary_unique.cxx cursor_secondary.hpp LIBRARY fpta testutils)
add_ut(fpta7_cursor_secondary_withdups TIMEOUT ${fpta7_cursor_secondary_withdups_timeout} SOURCE 7cursor_secondary_withdups.cxx cursor_secondary.hpp LIBRARY fpta testutils)
add_ut(fpta9_crud TIMEOUT ${fpta9_crud_timeout} SOURCE 9crud.cxx LIBRARY fpta testutils)
