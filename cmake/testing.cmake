find_package(GTest)
include(ExternalProject)

if(NOT GTEST_FOUND)
    message(STATUS "Lookup gtest sources...")
    find_path(GTEST_CMAKE_PROJ
        NAMES CMakeLists.txt
        PATHS /usr/src /usr/local /usr/local/src
        PATH_SUFFIXES gtest
        NO_DEFAULT_PATH NO_CMAKE_PATH)
    message(STATUS "GTEST_CMAKE_PROJ = ${GTEST_CMAKE_PROJ}")
    if(GTEST_CMAKE_PROJ)
        externalproject_add(GTEST PREFIX "${CMAKE_BINARY_DIR}/gtest" SOURCE_DIR "${GTEST_CMAKE_PROJ}" INSTALL_COMMAND "" CMAKE_ARGS "-DBUILD_SHARED_LIBS:BOOL=${BUILD_SHARED_LIBS}")
        set(GTEST_BUILTIN TRUE)
        set(GTEST_FOUND "${GTEST_CMAKE_PROJ}")

        get_filename_component(GTEST_DIR "${GTEST_CMAKE_PROJ}" PATH)
        if(NOT GTEST_INCLUDE_DIRS)
            find_path(GTEST_INCLUDE_DIRS gtest/gtest.h "${GTEST_DIR}")
        endif()

        set(GTEST_LIBDIR "${CMAKE_BINARY_DIR}/gtest/src/GTEST-build")
	if(BUILD_SHARED_LIBS)
            set(GTEST_LIBRARIES "${GTEST_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX}")
            set(GTEST_MAIN_LIBRARIES "${GTEST_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX}")
	else()
            set(GTEST_LIBRARIES "${GTEST_LIBDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
            set(GTEST_MAIN_LIBRARIES "${GTEST_LIBDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")
	endif()
        set(GTEST_BOTH_LIBRARIES ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
    endif()
endif()

if(GTEST_FOUND)
    include (CTest)
    enable_testing ()
    set(UT_INCLUDE_DIRECTORIES ${GTEST_INCLUDE_DIRS})
    set(UT_LIBRARIES ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    if(MEMORYCHECK_COMMAND OR CMAKE_MEMORYCHECK_COMMAND)
        add_custom_target(test_memcheck
            COMMAND ${CMAKE_CTEST_COMMAND}
            --force-new-ctest-process --test-action memcheck
            COMMAND ${CAT} "${CMAKE_BINARY_DIR}/Testing/Temporary/MemoryChecker.*.log")
    endif()
else()
    set(UT_INCLUDE_DIRECTORIES "")
    set(UT_LIBRARIES "")
endif()

function(add_gtest name)
    set(options DISABLED)
    set(oneValueArgs TIMEOUT PREFIX)
    set(multiValueArgs SOURCE LIBRARY INCLUDE_DIRECTORY DEPEND)
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

        if(GTEST_BUILTIN)
            add_dependencies(${target} GTEST)
        endif()

        if(params_DEPEND)
            add_dependencies(${target} ${params_DEPEND})
        endif()

        target_link_libraries(${target} ${UT_LIBRARIES})

        if(params_LIBRARY)
            target_link_libraries(${target} ${params_LIBRARY})
        endif()

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
