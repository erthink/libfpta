check_library_exists (gcov __gcov_flush  ""  HAVE_GCOV)

set(ENABLE_GCOV_DEFAULT OFF)
option(ENABLE_GCOV "Enable integration with gcov, a code coverage program" ${ENABLE_GCOV_DEFAULT})

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ENABLE_GPROF_DEFAULT ON)
else()
    set(ENABLE_GPROF_DEFAULT OFF)
endif()
option(ENABLE_GPROF "Enable integration with gprof, a performance analyzing tool" ${GPROF_DEFAULT})

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)
if (ENABLE_VALGRIND)
    check_include_file(valgrind/valgrind.h HAVE_VALGRIND_VALGRIND_H)
    if (NOT HAVE_VALGRIND_VALGRIND_H)
        message (FATAL_ERROR
             "ENABLE_VALGRIND option is set but valgrind/valgrind.h is not found")
        endif()
endif()
