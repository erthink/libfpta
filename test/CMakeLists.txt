include_directories(${UT_INCLUDE_DIRECTORIES})

if(BUILD_SHARED_LIBS)
  add_definitions("-DGTEST_LINKED_AS_SHARED_LIBRARY=1")
endif()

add_ut(fptu0_trivia TIMEOUT 1 SOURCE 0trivia.cxx LIBRARY fptu)
add_ut(fptu1_create TIMEOUT 1 SOURCE 1create.cxx LIBRARY fptu)
add_ut(fptu2_upsert TIMEOUT 1 SOURCE 2upsert.cxx LIBRARY fptu)
add_ut(fptu3_iterate TIMEOUT 1 SOURCE 3iterate.cxx LIBRARY fptu)
add_ut(fptu4_fetch TIMEOUT 1 SOURCE 4fetch.cxx LIBRARY fptu)
add_ut(fptu5_remove TIMEOUT 5 SOURCE 5remove.cxx shuffle6.hpp LIBRARY fptu)
add_ut(fptu6_shrink TIMEOUT 10 SOURCE 6shrink.cxx shuffle6.hpp LIBRARY fptu)
add_ut(fptu7_compare TIMEOUT 900 SOURCE 7compare.cxx shuffle6.hpp LIBRARY fptu)

# add_perf_test(abc_perf TIMEOUT 60 LIBRARY fptu)
# add_long_test(xyz_long TIMEOUT 600 LIBRARY fptu)

add_executable(fptu_c_mode c_mode.c)
target_link_libraries(fptu_c_mode fptu)
