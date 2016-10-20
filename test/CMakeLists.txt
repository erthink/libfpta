include_directories(${UT_INCLUDE_DIRECTORIES})

add_ut(0trivia TIMEOUT 1 SOURCE 0trivia.cxx LIBRARY fptu)
add_ut(1create TIMEOUT 1 SOURCE 1create.cxx LIBRARY fptu)
add_ut(2upsert TIMEOUT 1 SOURCE 2upsert.cxx LIBRARY fptu)
#add_ut(3loopup TIMEOUT 1 SOURCE 3lookup.cxx LIBRARY fptu)
#add_ut(4remove TIMEOUT 1 SOURCE 4remove.cxx LIBRARY fptu)
#add_ut(5shrink TIMEOUT 1 SOURCE 5shrink.cxx LIBRARY fptu)

# add_ut(emul TIMEOUT 1)
# add_ut(ensure TIMEOUT 1 LIBRARY fptu)

# add_perf_test(relptr_perf TIMEOUT 60 LIBRARY fptu)
# add_perf_test(mytex_perf TIMEOUT 600 LIBRARY fptu)
# add_long_test(jitter_long TIMEOUT 600 LIBRARY fptu)
# add_long_test(mytex_scrum TIMEOUT 100 LIBRARY fptu)
# add_perf_test(queue_perf TIMEOUT 600 LIBRARY fptu)
# add_perf_test(atomic_probe TIMEOUT 600 LIBRARY fptu)
# add_long_test(queue_long TIMEOUT 3600 LIBRARY fptu)

add_executable(c_mode c_mode.c)
target_link_libraries(c_mode fptu)
