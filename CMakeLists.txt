cmake_minimum_required(VERSION 2.8.2)
if(NOT CMAKE_VERSION VERSION_LESS 3.1.0)
  cmake_policy(SET CMP0054 NEW)
endif()

project(libfptu C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_INCLUDE_PATH})

include(CheckLibraryExists)
include(CheckIncludeFiles)
include(CheckCCompilerFlag)
include(CheckSymbolExists)
include(CheckCSourceRuns)
include(CheckCXXSourceRuns)
include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(TestBigEndian)
include(CheckFunctionExists)
include(FindPackageMessage)
include(CheckStructHasMember)

if(NOT CMAKE_VERSION VERSION_LESS 3.9.0)
  cmake_policy(SET CMP0069 NEW)
  include(CheckIPOSupport)
  check_ipo_support(RESULT CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE)
else()
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE FALSE)
endif()

#
# Set default build type to Debug. This is to ease a developer's
# life. Release binaries are built by BuildBot automatically anyway.
#
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING
    "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel."
    FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPERCASE)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(BUILD_SHARED_LIBS_DEFAULT OFF)
else()
  set(BUILD_SHARED_LIBS_DEFAULT ON)
endif()
option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)." ${BUILD_SHARED_LIBS_DEFAULT})

# Define GNU standard installation directories
# include(GNUInstallDirs)

include(cmake/utils.cmake)
# the order is significant: we need to know os and compiler to configure libs
include(cmake/arch.cmake)
include(cmake/os.cmake)
include(cmake/profile.cmake)
include(cmake/compiler.cmake)

if (CMAKE_CONFIGURATION_TYPES OR NOT CMAKE_BUILD_TYPE_UPPERCASE STREQUAL "DEBUG")
  set(INTERPROCEDURAL_OPTIMIZATION_DEFAULT ON)
else()
  set(INTERPROCEDURAL_OPTIMIZATION_DEFAULT OFF)
endif()

if(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE
    OR GCC_LTO_AVAILABLE OR MSVC_LTO_AVAILABLE OR CLANG_LTO_AVAILABLE)
  option(INTERPROCEDURAL_OPTIMIZATION "Enable interprocedural/LTO optimization." ${INTERPROCEDURAL_OPTIMIZATION_DEFAULT})
endif()

if(INTERPROCEDURAL_OPTIMIZATION)
  if(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE)
    message(STATUS "FPTU indulge Interprocedural Optimization by CMake")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
  elseif(GCC_LTO_AVAILABLE)
    message(STATUS "FPTU indulge Link-Time Optimization by GCC")
    set(CMAKE_AR ${CMAKE_GCC_AR} CACHE PATH "Path to ar program with LTO-plugin" FORCE)
    set(CMAKE_NM ${CMAKE_GCC_NM} CACHE PATH "Path to nm program with LTO-plugin" FORCE)
    set(CMAKE_RANLIB ${CMAKE_GCC_RANLIB} CACHE PATH "Path to ranlib program with LTO-plugin" FORCE)
    set(GCC_LTO_ENABLED TRUE)
  elseif(MSVC_LTO_AVAILABLE)
    set(MSVC_LTO_ENABLED TRUE)
    message(STATUS "FPTU indulge Link-Time Optimization by MSVC")
  elseif(CLANG_LTO_AVAILABLE)
    set(CLANG_LTO_ENABLED TRUE)
    if(CMAKE_CLANG_AR)
      set(CMAKE_AR ${CMAKE_CLANG_AR} CACHE PATH "Path to ar program with LTO-plugin" FORCE)
    endif()
    if(CMAKE_CLANG_RANLIB)
      set(CMAKE_RANLIB ${CMAKE_CLANG_RANLIB} CACHE PATH "Path to ranlib program with LTO-plugin" FORCE)
    endif()
    if(CMAKE_CLANG_NM)
      set(CMAKE_NM ${CMAKE_CLANG_NM} CACHE PATH "Path to nm program with LTO-plugin" FORCE)
    endif()
    message(STATUS "FPTU engage Link-Time Optimization by CLANG")
  else()
    message(WARNING "Unable to engage interprocedural/LTO optimization.")
  endif()
endif()

find_program(VALGRIND valgrind)
if(VALGRIND)
  # LY: cmake is ugly and nasty.
  #      - therefore memcheck-options should be defined before including ctest;
  #      - otherwise ctest may ignore it.
  set(MEMORYCHECK_SUPPRESSIONS_FILE
    "${PROJECT_SOURCE_DIR}/test/valgrind_suppress.txt"
    CACHE FILEPATH "Suppressions file for Valgrind" FORCE)
  set(MEMORYCHECK_COMMAND_OPTIONS
    "--trace-children=yes --leak-check=full --track-origins=yes --error-exitcode=42 --error-markers=@ --errors-for-leak-kinds=definite --fair-sched=yes --suppressions=${MEMORYCHECK_SUPPRESSIONS_FILE}"
    CACHE STRING "Valgrind options" FORCE)
  set(VALGRIND_COMMAND_OPTIONS "${MEMORYCHECK_COMMAND_OPTIONS}" CACHE STRING "Valgrind options" FORCE)
endif()
include(cmake/testing.cmake)

find_program(ECHO echo)
find_program(CAT cat)
find_program(GIT git)
find_program(LD ld)
find_program(CTAGS ctags)
find_program(CLANG_FORMAT clang-format)

check_function_exists(memmem HAVE_MEMMEM)
check_function_exists(memrchr HAVE_MEMRCHR)
check_function_exists(uuidgen HAVE_UUIDGEN)
CHECK_INCLUDE_FILES(unistd.h HAVE_UNISTD_H)
CHECK_INCLUDE_FILES(sys/uio.h HAVE_SYS_UIO_H)
CHECK_INCLUDE_FILES(sys/stat.h HAVE_SYS_STAT_H)
CHECK_STRUCT_HAS_MEMBER("struct timeval" tv_usec sys/select.h HAVE_TIMEVAL_TV_USEC)
CHECK_STRUCT_HAS_MEMBER("struct timespec" tv_nsec time.h HAVE_TIMESPEC_TV_NSEC)

CHECK_FUNCTION_EXISTS(pow NOT_NEED_LIBM)
if(NOT_NEED_LIBM)
  set(LIB_MATH "")
else()
  set(CMAKE_REQUIRED_LIBRARIES m)
  CHECK_FUNCTION_EXISTS(pow HAVE_LIBM)
  if(HAVE_LIBM)
    set(LIB_MATH m)
  else()
    message(FATAL_ERROR "No libm found for math support")
  endif()
endif()
CHECK_SYMBOL_EXISTS(nan math.h HAVE_nan)
CHECK_SYMBOL_EXISTS(nanf math.h HAVE_nanf)
CHECK_INCLUDE_FILES(ieee754.h HAVE_IEEE754_H)

#
# Enable 'make tags' target.
if(CTAGS)
  add_custom_target(tags COMMAND ${CTAGS} -R -f tags
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  add_custom_target(ctags DEPENDS tags)
endif(CTAGS)

#
# Enable 'make reformat' target.
if(CLANG_FORMAT)
  add_custom_target(reformat
    VERBATIM
    COMMAND git ls-files | grep -E \\.\(c|cxx|cc|cpp|h|hxx|hpp\)\$ | xargs clang-format -i --style=file
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif(CLANG_FORMAT)

include(cmake/multilib.cmake)

if(NOT "${PROJECT_BINARY_DIR}" STREQUAL "${PROJECT_SOURCE_DIR}")
  add_custom_target(distclean)
  add_custom_command(TARGET distclean
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${PROJECT_BINARY_DIR}"
    COMMENT "Removing the build directory and its content")
elseif(IS_DIRECTORY .git AND GIT)
  add_custom_target(distclean)
  add_custom_command(TARGET distclean
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMAND ${GIT} submodule foreach --recursive git clean -f -X -d
    COMMAND ${GIT} clean -f -X -d
    COMMENT "Removing all build files from the source directory")
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
setup_compile_flags()

#######################################################################

find_package(Threads REQUIRED)

if(BUILD_SHARED_LIBS)
  set(LIBFPTU_STATIC FALSE)
else()
  set(LIBFPTU_STATIC TRUE)
endif()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/fast_positive/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/fast_positive/config.h)
include_directories("${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")
add_subdirectory(src)
add_subdirectory(test)

set(PACKAGE "libfptu")
set(CPACK_PACKAGE_VERSION_MAJOR ${FPTU_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${FPTU_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${FPTU_VERSION_RELEASE})
set(CPACK_PACKAGE_VERSION_COMMIT ${FPTU_VERSION_REVISION})
set(PACKAGE_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}.${CPACK_PACKAGE_VERSION_COMMIT}")
message(STATUS "libfptu package version is ${PACKAGE_VERSION}")
