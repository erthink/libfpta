cmake_minimum_required(VERSION 2.8)

project(libfpta C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_INCLUDE_PATH})

include(CheckLibraryExists)
include(CheckIncludeFile)
include(CheckCCompilerFlag)
include(CheckSymbolExists)
include(CheckCSourceRuns)
include(CheckCXXSourceRuns)
include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(TestBigEndian)
include(CheckFunctionExists)
include(FindPackageMessage)

#
# Set default build type to Debug. This is to ease a developer's
# life. Release binaries are built by BuildBot automatically anyway.
#
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
        FORCE)
endif()

# Define GNU standard installation directories
# include(GNUInstallDirs)

include(cmake/utils.cmake)
# include(cmake/pod2man.cmake)
# the order is significant: we need to know os and compiler to configure libs
include(cmake/arch.cmake)
include(cmake/os.cmake)
include(cmake/compiler.cmake)
# include(cmake/atomic.cmake)
include(cmake/profile.cmake)
include(cmake/testing.cmake)

find_program(ECHO echo)
find_program(CAT cat)
find_program(GIT git)
find_program(LD ld)
find_program(CTAGS ctags)

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)

set (PACKAGE "libfpta")

# check_function_exists(memmem HAVE_MEMMEM)
# check_function_exists(memrchr HAVE_MEMRCHR)
# check_function_exists(uuidgen HAVE_UUIDGEN)

#
# Enable 'make tags' target.
#
add_custom_target(tags COMMAND ${CTAGS} -R -f tags
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(ctags DEPENDS tags)

#
# Get version
#
set (PACKAGE_VERSION "")
set (FPTU_VERSION "")

# Try to get version from VERSION file
set(VERSION_FILE_ORIG "${PROJECT_SOURCE_DIR}/VERSION")
set(VERSION_FILE "${PROJECT_BINARY_DIR}/VERSION")
if (EXISTS "${VERSION_FILE_ORIG}")
    file (STRINGS "${VERSION_FILE_ORIG}" FPTU_VERSION)
elseif (EXISTS "${VERSION_FILE}")
    file (STRINGS "${VERSION_FILE}" FPTU_VERSION)
endif()

# Get git version only if source directory has .git repository, this
# avoids git to search .git repository in parent
# directories.
#
if (EXISTS "${CMAKE_SOURCE_DIR}/.git" AND GIT)
    execute_process (COMMAND ${GIT} describe --tags --long HEAD
        OUTPUT_VARIABLE FPTU_GIT_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    if (NOT ("${FPTU_GIT_VERSION}" STREQUAL "${FPTU_VERSION}"))
        set(FPTU_VERSION "${FPTU_GIT_VERSION}")
        message(STATUS "Generating VERSION file")
        file(WRITE ${VERSION_FILE} "${FPTU_VERSION}\n")

        message(STATUS "Updating submodules")
        execute_process(COMMAND ${GIT} submodule update --init --recursive)
    endif()
endif()

if (NOT FPTU_VERSION)
    message (WARNING "Unable to retrive version from git or ${VERSION_FILE} file.")
    set(FPTU_VERSION "0.0.0.0")
endif()

#
# Split full version (git describe --long) to get components
#
string(REPLACE "-" "." FPTU_VERSION_LIST ${FPTU_VERSION})
string(REPLACE "." ";" FPTU_VERSION_LIST ${FPTU_VERSION_LIST})
LIST(GET FPTU_VERSION_LIST 0 CPACK_PACKAGE_VERSION_MAJOR)
LIST(GET FPTU_VERSION_LIST 1 CPACK_PACKAGE_VERSION_MINOR)
LIST(GET FPTU_VERSION_LIST 2 CPACK_PACKAGE_VERSION_PATCH)
LIST(GET FPTU_VERSION_LIST 3 CPACK_PACKAGE_VERSION_COMMIT)

set(PACKAGE_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_MINOR}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}")
set(PACKAGE_VERSION "${PACKAGE_VERSION}.${CPACK_PACKAGE_VERSION_COMMIT}")

find_package_message(FPTU_VERSION
    "libfpta version is ${FPTU_VERSION} (${PACKAGE_VERSION})"
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

setup_compile_flags()

#
# info summary (used in server version output)
#
set(FPTU_OPTIONS "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
set(FPTU_OPTIONS "${FPTU_OPTIONS} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}")
set(FPTU_BUILD "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}")
set(FPTU_C_COMPILER ${CMAKE_C_COMPILER})
set(FPTU_CXX_COMPILER ${CMAKE_CXX_COMPILER})
set(FPTU_C_FLAGS ${CMAKE_C_FLAGS})
set(FPTU_CXX_FLAGS ${CMAKE_CXX_FLAGS})

set(options VERSION BUILD C_COMPILER CXX_COMPILER C_FLAGS CXX_FLAGS
    ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND
    ENABLE_BACKTRACE
    HAVE_BFD)
foreach(option IN LISTS options)
    if (NOT DEFINED ${option})
        set(value "${FPTU_${option}}")
    else ()
        set(value "${${option}}")
    endif ()
    #message(STATUS "${option}: ${value}")
    find_package_message("${option}" "${option}: ${value}" "${value}")
endforeach(option)
message (STATUS "")

#######################################################################
