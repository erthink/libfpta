#
# Get version
fetch_version(FPTU "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message("libfptu version is ${FPTU_VERSION}")

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
  ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

target_link_libraries(fptu ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})

if(CC_HAS_VISIBILITY)
  target_compile_options(fptu PRIVATE "-fvisibility=hidden")
endif()

if(BUILD_SHARED_LIBS)
  target_compile_definitions(fptu PRIVATE "-Dfptu_EXPORTS")
endif()

#
# info summary (used in library version output)
get_target_property(FPTU_DEFINITIONS fptu COMPILE_DEFINITIONS)
string(REGEX REPLACE "([^;]+)" " -D\\1" FPTU_DEFINITIONS "${FPTU_DEFINITIONS}")
set(FPTU_C_COMPILER ${CMAKE_C_COMPILER})
set(FPTU_CXX_COMPILER ${CMAKE_CXX_COMPILER})
set(FPTU_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}} ${CMAKE_C_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
set(FPTU_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}} ${CMAKE_CXX_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
string(STRIP "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}" FPTU_BUILD_OPTIONS)
string(STRIP "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}" FPTU_BUILD_TAGRET)
string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTU_BUILD_COMPILER)
string(STRIP "${FPTU_CXX_FLAGS}${CMAKE_SHARED_LINKER_FLAGS}${FPTU_DEFINITIONS}" FPTU_BUILD_FLAGS)
string(TIMESTAMP FPTU_BUILD_TIMESTAMP UTC)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

set(options VERSION BUILD C_COMPILER CXX_COMPILER C_FLAGS CXX_FLAGS DEFINITIONS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND ENABLE_BACKTRACE
  HAVE_BFD)
foreach(option IN LISTS options)
  if(DEFINED "FPTU_${option}")
    set(value "${FPTU_${option}}")
  else()
    set(value "${${option}}")
  endif()
  find_package_message("FPTU_${option}" "FPTU_${option}: ${value}" "${value}")
endforeach(option)
