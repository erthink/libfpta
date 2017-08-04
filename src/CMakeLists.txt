##
##  Copyright 2016-2017 libfpta authors: please see AUTHORS file.
##
##  This file is part of libfpta, aka "Fast Positive Tables".
##
##  libfpta is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##
##  libfpta is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License for more details.
##
##  You should have received a copy of the GNU General Public License
##  along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
##

##
##  libfpta = { Fast Positive Tables, aka Позитивные Таблицы }
##
##  Ultra fast, compact, embeddable storage engine for (semi)structured data:
##  multiprocessing with zero-overhead, full ACID semantics with MVCC,
##  variety of indexes, saturation, sequences and much more.
##  Please see README.md at https://github.com/leo-yuriev/libfpta
##
##  The Future will Positive. Всё будет хорошо.
##
##  "Позитивные таблицы" предназначены для построения высокоскоростных
##  локальных хранилищ структурированных данных, с целевой производительностью
##  до 1.000.000 запросов в секунду на каждое ядро процессора.
##

#
# Get version
fetch_version(FPTA "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message(STATUS "libfpta version is ${FPTA_VERSION}")

add_library(fpta
  ${FAST_POSITIVE_CONFIG_H}
  ../fast_positive/tables.h
  ../fast_positive/tables_internal.h
  ../externals/libfptu/fast_positive/defs.h
  ../externals/libfptu/fast_positive/tuples.h
  ../externals/libfptu/fast_positive/tuples_internal.h
  ../externals/libmdbx/mdbx.h
  ../externals/t1ha/t1ha.h
  details.h
  osal.h
  composite.cxx
  common.cxx
  dbi.cxx
  table.cxx
  filter.cxx
  cursor.cxx
  schema.cxx
  index.cxx
  data.cxx
  misc.cxx
  inplace.cxx
  ${CMAKE_CURRENT_BINARY_DIR}/version.cxx
  )

set_target_properties(fpta PROPERTIES
  PROJECT_LABEL "Fast Positive Tables"
  FRAMEWORK TRUE
  VERSION "${FPTA_VERSION_MAJOR}.${FPTA_VERSION_MINOR}.${FPTA_VERSION_RELEASE}.${FPTA_VERSION_REVISION}"
  PUBLIC_HEADER "../fast_positive/tables.h;${FAST_POSITIVE_CONFIG_H}"
  PRIVATE_HEADER "../fast_positive/tables_internal.h"
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  )

target_link_libraries(fpta
  fptu mdbx t1ha ${LIB_MATH}
  ${CMAKE_THREAD_LIBS_INIT}
  )

if(UNIX AND NOT APPLE)
  target_link_libraries(fpta rt)
endif()

if(CC_HAS_VISIBILITY)
  target_compile_options(fpta PRIVATE "-fvisibility=hidden")
  if(LTO_ENABLED OR LTO_ENABLED)
    set_target_properties(fpta PROPERTIES LINK_FLAGS "-fvisibility=hidden")
  endif()
endif()

if(NOT LIBFPTA_STATIC)
  target_compile_definitions(fpta PRIVATE "-Dfpta_EXPORTS")
else()
  target_compile_definitions(fpta PRIVATE "-Dfpta_STATIC")
endif()

install(TARGETS fpta
  LIBRARY DESTINATION lib COMPONENT runtime
  ARCHIVE DESTINATION lib/static COMPONENT devel
  PUBLIC_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
  PRIVATE_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
  )

###############################################################################
#
# library build info (used in library version output)
#

# get definitions as a string of "-Dxyz=124 ..."
get_target_property(FPTA_DEFINITIONS fpta COMPILE_DEFINITIONS)
if(NOT FPTA_DEFINITIONS)
  set(FPTA_DEFINITIONS "")
endif()
list(REMOVE_DUPLICATES FPTA_DEFINITIONS)
set(FPTA_DEFINITIONS_VERSION ${FPTA_DEFINITIONS})
string(REGEX REPLACE "([^;]+)" " -D\\1" FPTA_DEFINITIONS "${FPTA_DEFINITIONS}")
string(STRIP FPTA_DEFINITIONS "${FPTA_DEFINITIONS}")

# get target compile options as a list
get_target_property(fpta_compile_options fpta COMPILE_OPTIONS)
if(NOT fpta_compile_options)
  set(fpta_compile_options "")
endif()
# append cmake's common cxx flags and defines

string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS}" "${CMAKE_CXX_DEFINES}")
list(INSERT fpta_compile_options 0 "${cmake_cxx_options}")
unset(cmake_cxx_options)

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}" "${CMAKE_CXX_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
  list(APPEND fpta_compile_options "${cmake_cxx_options}")
  unset(cmake_cxx_options)
endif()

# append linker dll's options
if(NOT LIBFPTA_STATIC)
  string(REPLACE " " ";" cmake_shared_linker_options "${CMAKE_SHARED_LINKER_FLAGS}")
  list(APPEND fpta_compile_options ${cmake_shared_linker_options})
  unset(cmake_shared_linker_options)
endif()

# drop duplicates in the option list
list(REMOVE_DUPLICATES fpta_compile_options)

# make string of space separated flags
string(REPLACE ";" " " FPTA_COMPILE_FLAGS "${fpta_compile_options}")
unset(fpta_compile_options)
string(STRIP "${FPTA_COMPILE_FLAGS}${FPTA_DEFINITIONS}" FPTA_COMPILE_FLAGS)
if(CMAKE_CONFIGURATION_TYPES)
  # add dynamic part via per-configuration define
  message(STATUS "FPTA Compile Flags: ${FPTA_COMPILE_FLAGS} <AND CONFIGURATION DEPENDENT>")
  set(FPTA_COMPILE_FLAGS "FPTA_COMPILE_FLAGS \"${FPTA_COMPILE_FLAGS}\"")
  add_definitions(
    -DFPTA_COMPILE_FLAGS="$<$<CONFIG:Debug>:${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_CXX_FLAGS_MINSIZEREL} ${CMAKE_CXX_DEFINES_MINSIZEREL}>"
    )
else()
  message(STATUS "FPTA Compile Flags: ${FPTA_COMPILE_FLAGS}")
  set(FPTA_COMPILE_FLAGS "\"${FPTA_COMPILE_FLAGS}\"")
endif()

# make a build-target triplet
if(CMAKE_CONFIGURATION_TYPES)
  # via per-configuration define
  add_definitions(-DFPTA_BUILD_TAGRET="${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-$<CONFIG>")
  set(FPTA_BUILD_TAGRET FPTA_BUILD_TAGRET)
else()
  string(STRIP "\"${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}\"" FPTA_BUILD_TAGRET)
endif()

# generate version file
string(TIMESTAMP FPTA_BUILD_TIMESTAMP UTC)
string(REPLACE "  " " " FPTA_CMAKE_OPTIONS "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
  " -DFPTA_ALLOW_DOT4NAMES=${FPTA_ALLOW_DOT4NAMES}"
  " -DFPTA_PROHIBIT_UPSERT_NAN=${FPTA_PROHIBIT_UPSERT_NAN}"
  " -DFPTA_PROHIBIT_UPSERT_DENIL=${FPTA_PROHIBIT_UPSERT_DENIL}"
  " -DFPTA_CLEAN_DENIL=${FPTA_CLEAN_DENIL}"
  " -DFPTA_PROHIBIT_NEARBY4UNORDERED=${FPTA_PROHIBIT_NEARBY4UNORDERED}"
  " -DFPTA_PROHIBIT_LOSS_PRECISION=${FPTA_PROHIBIT_LOSS_PRECISION}"
  " -DFPTA_ENABLE_RETURN_INTO_RANGE=${FPTA_ENABLE_RETURN_INTO_RANGE}"
  " -DFPTA_ENABLE_ABORT_ON_PANIC=${FPTA_ENABLE_ABORT_ON_PANIC}"
  " -DT1HA_USE_FAST_ONESHOT_READ=${T1HA_USE_FAST_ONESHOT_READ}"
  " -DENABLE_ASAN=${ENABLE_ASAN}"
  " -DENABLE_VALGRIND=${ENABLE_VALGRIND}"
  " -DENABLE_GPROF=${ENABLE_GPROF}"
  " -DENABLE_GCOV=${ENABLE_GCOV}"
  " -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}"
  )

string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTA_BUILD_COMPILER)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

set(options VERSION BUILD C_COMPILER CXX_COMPILER DEFINITIONS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND ENABLE_BACKTRACE BUILD_TARGET
  HAVE_BFD ENABLE_ASAN)
foreach(option IN LISTS options)
  if(DEFINED "FPTA_${option}")
    set(value "${FPTA_${option}}")
  elseif(DEFINED "${option}")
    set(value "${${option}}")
  else()
    set(value "${CMAKE_${option}}")
  endif()
  find_package_message("FPTA_${option}" "FPTA_${option}: ${value}" "${value}")
endforeach(option)
