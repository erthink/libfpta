add_library(fptu STATIC
    ../fast_positive/defs.h
    ../fast_positive/tuples.h
    ../fast_positive/internals.h
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
)
