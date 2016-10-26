include_directories(${UT_INCLUDE_DIRECTORIES})

add_ut(0trivia TIMEOUT 1 SOURCE 0trivia.cxx LIBRARY fptu)
add_ut(1create TIMEOUT 1 SOURCE 1create.cxx LIBRARY fptu)
add_ut(2upsert TIMEOUT 1 SOURCE 2upsert.cxx LIBRARY fptu)
add_ut(3iterate TIMEOUT 1 SOURCE 3iterate.cxx LIBRARY fptu)
add_ut(4fetch TIMEOUT 1 SOURCE 4fetch.cxx LIBRARY fptu)
add_ut(5remove TIMEOUT 5 SOURCE 5remove.cxx shuffle6.hpp LIBRARY fptu)
#add_ut(6shrink TIMEOUT 1 SOURCE 6shrink.cxx LIBRARY fptu)

# add_perf_test(relptr_perf TIMEOUT 60 LIBRARY fptu)
# add_long_test(jitter_long TIMEOUT 600 LIBRARY fptu)

add_executable(c_mode c_mode.c)
target_link_libraries(c_mode fptu)
