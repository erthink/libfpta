#
# Get version
fetch_version(FPTU "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message(STATUS "libfptu version is ${FPTU_VERSION}")

add_library(fptu
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

if(UNIX AND NOT APPLE)
  target_link_libraries(fptu rt)
endif()

if(CC_HAS_VISIBILITY)
  target_compile_options(fptu PRIVATE "-fvisibility=hidden")
  if(GCC_LTO_ENABLED OR CLANG_LTO_ENABLED)
    set_target_properties(fptu PROPERTIES LINK_FLAGS "-fvisibility=hidden")
  endif()
endif()

if(NOT LIBFPTU_STATIC)
  target_compile_definitions(fptu PRIVATE "-Dfptu_EXPORTS")
else()
  target_compile_definitions(fptu PRIVATE "-Dfptu_STATIC")
endif()

###############################################################################
#
# library build info (used in library version output)
#

# get definitions as a string of "-Dxyz=124 ..."
get_target_property(FPTU_DEFINITIONS fptu COMPILE_DEFINITIONS)
if(NOT FPTU_DEFINITIONS)
  set(FPTU_DEFINITIONS "")
endif()
list(REMOVE_DUPLICATES FPTU_DEFINITIONS)
set(FPTU_DEFINITIONS_VERSION ${FPTU_DEFINITIONS})
string(REGEX REPLACE "([^;]+)" " -D\\1" FPTU_DEFINITIONS "${FPTU_DEFINITIONS}")
string(STRIP FPTU_DEFINITIONS "${FPTU_DEFINITIONS}")

# get target compile options as a list
get_target_property(fptu_compile_options fptu COMPILE_OPTIONS)
if(NOT fptu_compile_options)
  set(fptu_compile_options "")
endif()
# append cmake's common cxx flags and defines

string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS}" "${CMAKE_CXX_DEFINES}")
list(INSERT fptu_compile_options 0 "${cmake_cxx_options}")
unset(cmake_cxx_options)

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}" "${CMAKE_CXX_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
  list(APPEND fptu_compile_options "${cmake_cxx_options}")
  unset(cmake_cxx_options)
endif()

# append linker dll's options
if(NOT LIBFPTU_STATIC)
  string(REPLACE " " ";" cmake_shared_linker_options "${CMAKE_SHARED_LINKER_FLAGS}")
  list(APPEND fptu_compile_options ${cmake_shared_linker_options})
  unset(cmake_shared_linker_options)
endif()

# drop duplicates in the option list
list(REMOVE_DUPLICATES fptu_compile_options)

# make string of space separated flags
string(REPLACE ";" " " FPTU_COMPILE_FLAGS "${fptu_compile_options}")
unset(fptu_compile_options)
string(STRIP "${FPTU_COMPILE_FLAGS}${FPTU_DEFINITIONS}" FPTU_COMPILE_FLAGS)
if(CMAKE_CONFIGURATION_TYPES)
  # add dynamic part via per-configuration define
  message(STATUS "FPTU Compile Flags: ${FPTU_COMPILE_FLAGS} <AND CONFIGURATION DEPENDENT>")
  set(FPTU_COMPILE_FLAGS "FPTU_COMPILE_FLAGS \"${FPTU_COMPILE_FLAGS}\"")
  add_definitions(
   -DFPTU_COMPILE_FLAGS="$<$<CONFIG:Debug>:${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_CXX_FLAGS_MINSIZEREL} ${CMAKE_CXX_DEFINES_MINSIZEREL}>"
  )
else()
  message(STATUS "FPTU Compile Flags: ${FPTU_COMPILE_FLAGS}")
  set(FPTU_COMPILE_FLAGS "\"${FPTU_COMPILE_FLAGS}\"")
endif()

# make a build-target triplet
if(CMAKE_CONFIGURATION_TYPES)
  # via per-configuration define
  add_definitions(-DFPTU_BUILD_TAGRET="${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-$<CONFIG>")
  set(FPTU_BUILD_TAGRET FPTU_BUILD_TAGRET)
else()
  string(STRIP "\"${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}\"" FPTU_BUILD_TAGRET)
endif()

# generate version file
string(TIMESTAMP FPTU_BUILD_TIMESTAMP UTC)
string(STRIP "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}" FPTU_CMAKE_OPTIONS)
string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTU_BUILD_COMPILER)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

set(options VERSION BUILD C_COMPILER CXX_COMPILER DEFINITIONS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND ENABLE_BACKTRACE BUILD_TARGET
  HAVE_BFD ENABLE_ASAN)
foreach(option IN LISTS options)
  if(DEFINED "FPTU_${option}")
    set(value "${FPTU_${option}}")
  elseif(DEFINED "${option}")
    set(value "${${option}}")
  else()
    set(value "${CMAKE_${option}}")
  endif()
  find_package_message("FPTU_${option}" "FPTU_${option}: ${value}" "${value}")
endforeach(option)
