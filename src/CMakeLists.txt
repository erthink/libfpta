add_library(fptu SHARED
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
    data.cxx
)

target_link_libraries(fptu ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})

if (CC_HAS_VISIBILITY)
   target_compile_options(fptu PRIVATE "-fvisibility=hidden")
endif()
