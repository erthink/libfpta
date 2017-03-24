## Copyright 2010-2017 libfptu authors: please see AUTHORS file.
##
## This file is part of libfptu, aka "Fast Positive Tables".
##
## libfptu is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## libfptu is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
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
    message(STATUS "Found GoogleTest sources at ${gtest_root}, attach as ExternalProject")
    unset(GTEST_INCLUDE_DIR CACHE)
    unset(GTEST_LIBRARY CACHE)
    unset(GTEST_LIBRARY_DEBUG CACHE)
    unset(GTEST_MAIN_LIBRARY CACHE)
    unset(GTEST_MAIN_LIBRARY_DEBUG CACHE)
    unset(GTEST_BOTH_LIBRARIES CACHE)
    unset(GTEST_FOUND CACHE)

    include(ExternalProject)
    externalproject_add(
      GoogleTest PREFIX "${CMAKE_BINARY_DIR}/gtest"
      SOURCE_DIR ${gtest_root}
      INSTALL_COMMAND ""
      CMAKE_ARGS "-DBUILD_SHARED_LIBS:BOOL=${BUILD_SHARED_LIBS}" "-DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}" "-Dgtest_force_shared_crt:BOOL=ON"
      )

    ExternalProject_Get_Property(GoogleTest source_dir)
    set(GTEST_INCLUDE_DIR ${source_dir}/include)
    unset(source_dir)

    ExternalProject_Get_Property(GoogleTest binary_dir)
    if(CMAKE_CONFIGURATION_TYPES)
      set(binary_dir ${binary_dir}/${CMAKE_BUILD_TYPE})
    endif()
    if(BUILD_SHARED_LIBS)
      add_library(gtest SHARED IMPORTED)
      add_library(gtest_main SHARED IMPORTED)
      set(GTEST_LIBRARY ${binary_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX})
      set(GTEST_MAIN_LIBRARY ${binary_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX})
      if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
        set_property(TARGET gtest PROPERTY IMPORTED_IMPLIB ${binary_dir}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest${CMAKE_IMPORT_LIBRARY_SUFFIX})
        set_property(TARGET gtest_main PROPERTY IMPORTED_IMPLIB ${binary_dir}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest_main${CMAKE_IMPORT_LIBRARY_SUFFIX})
      endif()
    else()
      add_library(gtest STATIC IMPORTED)
      add_library(gtest_main STATIC IMPORTED)
      set(GTEST_LIBRARY ${binary_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX})
      set(GTEST_MAIN_LIBRARY ${binary_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX})
    endif()
    unset(binary_dir)
    add_dependencies(gtest GoogleTest)
    add_dependencies(gtest_main GoogleTest)

    set_property(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARY})
    set_property(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY})

    set(GTEST_BOTH_LIBRARIES gtest gtest_main)
    set(GTEST_FOUND TRUE)

    # message(STATUS "GTEST_INCLUDE_DIR = ${GTEST_INCLUDE_DIR}")
    # message(STATUS "GTEST_LIBRARY = ${GTEST_LIBRARY}")
    # message(STATUS "GTEST_MAIN_LIBRARY = ${GTEST_MAIN_LIBRARY}")
    # message(STATUS "GTEST_BOTH_LIBRARIES = ${GTEST_BOTH_LIBRARIES}")
  else()
    message(STATUS "NOT FOUND GoogleTest at paths ${gtest_paths}, testing not be enabled")
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
