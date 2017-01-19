add_library(fptu STATIC
    ../fast_positive/defs.h
    ../fast_positive/tuples.h
    ../fast_positive/tuples_internal.h
    common.cxx
    create.cxx
    check.cxx
    upsert.cxx
    remove.cxx
    misc.cxx
    shrink.cxx
    get.cxx
    compare.cxx
    iterator.cxx
    sort.cxx
    time.cxx
)

target_link_libraries(fptu ${CMAKE_THREAD_LIBS_INIT})
