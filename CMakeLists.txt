cmake_minimum_required(VERSION 2.8)
cmake_policy(SET CMP0054 NEW)

project(libfpta C CXX)

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

#
# Set default build type to Debug. This is to ease a developer's
# life. Release binaries are built by BuildBot automatically anyway.
#
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
        FORCE)
endif()
option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)." ON)

# Define GNU standard installation directories
# include(GNUInstallDirs)

include(cmake/utils.cmake)
# the order is significant: we need to know os and compiler to configure libs
include(cmake/arch.cmake)
include(cmake/os.cmake)
include(cmake/profile.cmake)
include(cmake/compiler.cmake)
# include(cmake/atomic.cmake)
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

#
# Enable 'make tags' target.
if (CTAGS)
    add_custom_target(tags COMMAND ${CTAGS} -R -f tags
       WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
	add_custom_target(ctags DEPENDS tags)
endif(CTAGS)

#
# Enable 'make reformat' target.
if (CLANG_FORMAT)
    add_custom_target(reformat
        VERBATIM
        COMMAND git ls-files | grep -E \\.\(c|cxx|cc|cpp|h|hxx|hpp\)\$ | xargs clang-format -i --style=file
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif(CLANG_FORMAT)

#
# Get version
set (PACKAGE_VERSION "")
set (FPTA_VERSION "")

# Try to get version from VERSION file
set(VERSION_FILE_ORIG "${PROJECT_SOURCE_DIR}/VERSION")
set(VERSION_FILE "${PROJECT_BINARY_DIR}/VERSION")
if (EXISTS "${VERSION_FILE_ORIG}")
    file (STRINGS "${VERSION_FILE_ORIG}" FPTA_VERSION)
elseif (EXISTS "${VERSION_FILE}")
    file (STRINGS "${VERSION_FILE}" FPTA_VERSION)
endif()

# Get git version only if source directory has .git repository, this
# avoids git to search .git repository in parent
# directories.
#
if (EXISTS "${CMAKE_SOURCE_DIR}/.git" AND GIT)
    execute_process (COMMAND ${GIT} describe --tags --long HEAD
        OUTPUT_VARIABLE FPTA_GIT_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    if (NOT ("${FPTA_GIT_VERSION}" STREQUAL "${FPTA_VERSION}"))
        set(FPTA_VERSION "${FPTA_GIT_VERSION}")
        message(STATUS "Generating VERSION file")
        file(WRITE ${VERSION_FILE} "${FPTA_VERSION}\n")

        message(STATUS "Updating submodules")
        execute_process(COMMAND ${GIT} submodule update --init --recursive)
    endif()
endif()

if (NOT FPTA_VERSION)
    message (WARNING "Unable to retrive version from git or ${VERSION_FILE} file.")
    set(FPTA_VERSION "0.0.0.0")
endif()

#
# Split full version (git describe --long) to get components
#
string(REPLACE "-" "." FPTA_VERSION_LIST ${FPTA_VERSION})
string(REPLACE "." ";" FPTA_VERSION_LIST ${FPTA_VERSION_LIST})
LIST(GET FPTA_VERSION_LIST 0 CPACK_PACKAGE_VERSION_MAJOR)
LIST(GET FPTA_VERSION_LIST 1 CPACK_PACKAGE_VERSION_MINOR)
LIST(GET FPTA_VERSION_LIST 2 CPACK_PACKAGE_VERSION_PATCH)
LIST(GET FPTA_VERSION_LIST 3 CPACK_PACKAGE_VERSION_COMMIT)

set(PACKAGE_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_MINOR}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_COMMIT}")

find_package_message(FPTA_VERSION
    "libfpta version is ${FPTA_VERSION} (${PACKAGE_VERSION})"
    "${PACKAGE_VERSION}")

include(cmake/multilib.cmake)

if(NOT "${PROJECT_BINARY_DIR}" STREQUAL "${PROJECT_SOURCE_DIR}")
    add_custom_target(distclean)
    add_custom_command(TARGET distclean
        COMMAND ${CMAKE_COMMAND} -E remove_directory "${PROJECT_BINARY_DIR}"
        COMMENT "Removing the build directory and its content"
    )
elseif(IS_DIRECTORY .git AND GIT)
    add_custom_target(distclean)
    add_custom_command(TARGET distclean
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND ${GIT} submodule foreach --recursive git clean -f -X -d
        COMMAND ${GIT} clean -f -X -d
        COMMENT "Removing all build files from the source directory"
    )
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
setup_compile_flags()

#
# info summary (used in server version output)
#
set(FPTA_OPTIONS "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
set(FPTA_OPTIONS "${FPTA_OPTIONS} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}")
set(FPTA_BUILD "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}")
set(FPTA_C_COMPILER ${CMAKE_C_COMPILER})
set(FPTA_CXX_COMPILER ${CMAKE_CXX_COMPILER})
set(FPTA_C_FLAGS ${CMAKE_C_FLAGS})
set(FPTA_CXX_FLAGS ${CMAKE_CXX_FLAGS})

set(options VERSION BUILD C_COMPILER CXX_COMPILER C_FLAGS CXX_FLAGS
    ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND
    ENABLE_BACKTRACE
    HAVE_BFD)
foreach(option IN LISTS options)
    if (NOT DEFINED ${option})
        set(value "${FPTA_${option}}")
    else ()
        set(value "${${option}}")
    endif ()
    #message(STATUS "${option}: ${value}")
    find_package_message("${option}" "${option}: ${value}" "${value}")
endforeach(option)
message (STATUS "")

#######################################################################

find_package(Threads REQUIRED)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/fast_positive/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/fast_positive/config.h)

set(PACKAGE "libfpta")

# Specify where to look for include files.
include_directories("${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" "${PROJECT_SOURCE_DIR}/submodules/libfptu" "${PROJECT_SOURCE_DIR}/submodules")

add_subdirectory(submodules)
add_subdirectory(src)
add_subdirectory(test)
