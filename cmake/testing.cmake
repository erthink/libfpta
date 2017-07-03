## Copyright 2010-2017 libfpta authors: please see AUTHORS file.
##
## This file is part of libfpta, aka "Fast Positive Tables".
##
## libfpta is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## libfpta is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
##

# Expected GTest was already found and/or pointed via ${gtest_root},
# otherwise will search at ${gtest_paths} locations, if defined or default ones.
find_package(GTest)

if(NOT GTEST_FOUND)
  message(STATUS "Lookup GoogleTest sources...")
  if(NOT gtest_paths)
    if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
      set(gtest_paths
        $ENV{SystemDrive}/
        $ENV{SystemDrive}/Source $ENV{USERPROFILE}/Source $ENV{PUBLIC}/Source
        $ENV{SystemDrive}/Source/Repos $ENV{USERPROFILE}/Source/Repos $ENV{PUBLIC}/Source/Repos)
    else()
      set(gtest_paths /usr/src /usr/local /usr/local/src)
    endif()
  endif()
  # message(STATUS "gtest_paths = ${gtest_paths}")

  find_path(gtest_root
    NAMES CMakeLists.txt
    PATHS ${gtest_paths}
    PATH_SUFFIXES googletest/googletest gtest/googletest gtest
    NO_DEFAULT_PATH NO_CMAKE_PATH)

  if(gtest_root)
    message(STATUS "Found GoogleTest sources at ${gtest_root}")
  else()
    # Download and unpack GoogleTest at configure time
    configure_file(${CMAKE_SOURCE_DIR}/cmake/googletest-download.cmake.in googletest-download/CMakeLists.txt)
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
      RESULT_VARIABLE result
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download)
    if(result)
      message(FATAL_ERROR "CMake step for GoogleTest failed: ${result}")
    else()
      execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
      if(result)
        message(FATAL_ERROR "Build step for GoogleTest failed: ${result}")
      else()
        set(gtest_root "${CMAKE_BINARY_DIR}/googletest-src/googletest")
      endif()
    endif()
  endif()

  if(gtest_root)
    unset(GTEST_INCLUDE_DIR CACHE)
    unset(GTEST_LIBRARY CACHE)
    unset(GTEST_LIBRARY_DEBUG CACHE)
    unset(GTEST_MAIN_LIBRARY CACHE)
    unset(GTEST_MAIN_LIBRARY_DEBUG CACHE)
    unset(GTEST_BOTH_LIBRARIES CACHE)
    unset(GTEST_FOUND CACHE)

    # Prevent overriding the parent project's compiler/linker
    # settings on Windows
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    # Add googletest directly to our build. This defines
    # the gtest and gtest_main targets.
    add_subdirectory(${gtest_root}
      ${CMAKE_BINARY_DIR}/googletest-build)

    if(CC_HAS_WERROR)
      if(MSVC)
        target_compile_options(gtest PRIVATE "/WX-")
        target_compile_options(gtest_main PRIVATE "/WX-")
      else()
        target_compile_options(gtest PRIVATE "-Wno-error")
        target_compile_options(gtest_main PRIVATE "-Wno-error")
      endif()
    endif()

    # The gtest/gtest_main targets carry header search path
    # dependencies automatically when using CMake 2.8.11 or
    # later. Otherwise we have to add them here ourselves.
    if(CMAKE_VERSION VERSION_LESS 2.8.11)
      set(GTEST_INCLUDE_DIR "${gtest_SOURCE_DIR}/include")
    else()
      set(GTEST_INCLUDE_DIR "")
    endif()

    set(GTEST_BOTH_LIBRARIES gtest gtest_main)
    set(GTEST_FOUND TRUE)
  endif()
endif()

if(GTEST_FOUND)
  include(CTest)
  enable_testing()
  set(UT_INCLUDE_DIRECTORIES ${GTEST_INCLUDE_DIR})
  set(UT_LIBRARIES ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
  if(MEMORYCHECK_COMMAND OR CMAKE_MEMORYCHECK_COMMAND)
    add_custom_target(test_memcheck
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process --test-action memcheck
      COMMAND ${CAT} "${CMAKE_BINARY_DIR}/Testing/Temporary/MemoryChecker.*.log")
  endif()
else()
  set(UT_INCLUDE_DIRECTORIES "")
  set(UT_LIBRARIES "")
  message(STATUS "GoogleTest NOT available, so testing NOT be ENABLED")
endif()

function(add_gtest name)
  set(options DISABLED)
  set(oneValueArgs TIMEOUT PREFIX)
  set(multiValueArgs SOURCE LIBRARY INCLUDE_DIRECTORY DEPEND DLLPATH)
  cmake_parse_arguments(params "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if(params_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "Unknown keywords given to add_gtest(): \"${params_UNPARSED_ARGUMENTS}\".")
  endif()

  if(GTEST_FOUND)
    macro(oops)
      message(FATAL_ERROR "add_gtest(): Opps, " ${ARGV})
    endmacro()

    if(NOT params_SOURCE)
      set(params_SOURCE ${name}.cpp)
    endif()

    set(target "${params_PREFIX}${name}")
    add_executable(${target} ${params_SOURCE})

    if(params_DEPEND)
      add_dependencies(${target} ${params_DEPEND})
    endif()

    target_link_libraries(${target} ${UT_LIBRARIES})

    if(params_LIBRARY)
      target_link_libraries(${target} ${params_LIBRARY})
    endif()

    foreach(dep IN LISTS params_LIBRARY GTEST_BOTH_LIBRARIES)
      get_target_property(type ${dep} TYPE)
      if(${type} STREQUAL "SHARED_LIBRARY")
        get_target_property(filename ${dep} IMPORTED_LOCATION)
        if(NOT filename)
          set(dir $<TARGET_FILE_DIR:${dep}>)
          #get_target_property(filename ${dep} LOCATION)
          #get_filename_component(dir ${filename} DIRECTORY)
        else()
          get_filename_component(dir ${filename} DIRECTORY)
        endif()
        list(APPEND params_DLLPATH ${dir})
      endif()
    endforeach(dep)

    if(params_INCLUDE_DIRECTORY)
      set_target_properties(${target} PROPERTIES INCLUDE_DIRECTORIES ${params_INCLUDE_DIRECTORY})
    endif()

    if(NOT params_DISABLED)
      add_test(${name} ${target})
      if(params_TIMEOUT)
        if(MEMORYCHECK_COMMAND OR CMAKE_MEMORYCHECK_COMMAND)
          # FIXME: unless there are any other ideas how to fix the
          #        timeouts problem when testing under Valgrind.
          math(EXPR params_TIMEOUT "${params_TIMEOUT} * 42")
        endif()
        set_tests_properties(${name} PROPERTIES TIMEOUT ${params_TIMEOUT})
      endif()
      if(params_DLLPATH)
        list(REMOVE_DUPLICATES params_DLLPATH)
        set_tests_properties(${name} PROPERTIES ENVIRONMENT "PATH=${params_DLLPATH};$ENV{PATH}")
      endif()
    endif()
  endif()
endfunction(add_gtest)

function(add_ut name)
  add_gtest(${name} PREFIX "ut_" ${ARGN})
endfunction(add_ut)

function(add_long_test name)
  add_gtest(${name} PREFIX "lt_" DISABLED ${ARGN})
endfunction(add_long_test)

function(add_perf_test name)
  add_gtest(${name} PREFIX "pt_" DISABLED ${ARGN})
endfunction(add_perf_test)
