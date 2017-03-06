#
# Get version
fetch_version(FPTU "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message("libfptu version is ${FPTU_VERSION}")

#
# info summary (used in library version output)
set(FPTU_C_COMPILER ${CMAKE_C_COMPILER})
set(FPTU_CXX_COMPILER ${CMAKE_CXX_COMPILER})
set(FPTU_C_FLAGS ${CMAKE_C_FLAGS})
set(FPTU_CXX_FLAGS ${CMAKE_CXX_FLAGS})
string(STRIP "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}" FPTU_BUILD_OPTIONS)
string(STRIP "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}" FPTU_BUILD_TAGRET)
string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTU_BUILD_COMPILER)
string(STRIP "${FPTU_CXX_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS}" FPTU_BUILD_FLAGS)
string(TIMESTAMP FPTU_BUILD_TIMESTAMP UTC)

set(options VERSION BUILD C_COMPILER CXX_COMPILER C_FLAGS CXX_FLAGS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND
  ENABLE_BACKTRACE
  HAVE_BFD)
foreach(option IN LISTS options)
  if(NOT DEFINED ${option})
    set(value "${FPTU_${option}}")
  else()
    set(value "${${option}}")
  endif()
  find_package_message("FPTU_${option}" "FPTU_${option}: ${value}" "${value}")
endforeach(option)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

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
    ${CMAKE_CURRENT_BINARY_DIR}/version.cxx
)

target_link_libraries(fptu ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})

if (CC_HAS_VISIBILITY)
   target_compile_options(fptu PRIVATE "-fvisibility=hidden")
endif()
