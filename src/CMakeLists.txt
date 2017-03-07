#
# Get version
fetch_version(FPTA "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message("libfpta version is ${FPTA_VERSION}")

#
# info summary (used in library version output)
set(FPTA_C_COMPILER ${CMAKE_C_COMPILER})
set(FPTA_CXX_COMPILER ${CMAKE_CXX_COMPILER})
set(FPTA_C_FLAGS ${CMAKE_C_FLAGS})
set(FPTA_CXX_FLAGS ${CMAKE_CXX_FLAGS})
string(STRIP "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}" FPTA_BUILD_OPTIONS)
string(STRIP "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}" FPTA_BUILD_TAGRET)
string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTA_BUILD_COMPILER)
string(STRIP "${FPTA_CXX_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS}" FPTA_BUILD_FLAGS)
string(TIMESTAMP FPTA_BUILD_TIMESTAMP UTC)

set(options VERSION BUILD C_COMPILER CXX_COMPILER C_FLAGS CXX_FLAGS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND
  ENABLE_BACKTRACE
  HAVE_BFD)
foreach(option IN LISTS options)
  if (NOT DEFINED ${option})
    set(value "${FPTA_${option}}")
  else()
    set(value "${${option}}")
  endif()
  find_package_message("FPTA_${option}" "FPTA_${option}: ${value}" "${value}")
endforeach(option)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

add_library(fpta SHARED
   ../fast_positive/tables.h
   ../fast_positive/tables_internal.h
   common.cxx
   filter.cxx
   cursor.cxx
   schema.cxx
   index.cxx
   data.cxx
   secondary.cxx
   misc.cxx
   ${CMAKE_CURRENT_BINARY_DIR}/version.cxx
)

if (CC_HAS_VISIBILITY)
   target_compile_options(fpta PRIVATE "-fvisibility=hidden")
endif()

if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
   target_link_libraries(fpta mdbx)
endif()

target_link_libraries(fpta
    fptu t1ha ${LIB_MATH}
    ${CMAKE_THREAD_LIBS_INIT}
)
