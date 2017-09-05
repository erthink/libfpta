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

include_directories(${UT_INCLUDE_DIRECTORIES})

if(BUILD_SHARED_LIBS)
  add_definitions("-DGTEST_LINKED_AS_SHARED_LIBRARY=1")
endif()

add_library(testutils STATIC
  keygen.cxx
  keygen.hpp
  fpta_test.h
  tools.hpp
  crutch4windows.cxx)
target_link_libraries(testutils ${UT_LIBRARIES})

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(fpta_small_timeout 10)
  set(fpta3_smoke_timeout 20)
  set(fpta5_key_timeout 30)
  set(fpta6_index_primary_timeout 200)
  set(fpta6_index_secondary_timeout 1500)
  set(fpta7_cursor_primary_timeout 600)
  set(fpta7_cursor_secondary_unique_timeout 3000)
  set(fpta7_cursor_secondary_withdups_timeout 3000)
  set(fpta8_composite 300)
  set(fpta9_crud_timeout 300)
else()
  set(fpta_small_timeout 3)
  set(fpta3_smoke_timeout 10)
  set(fpta5_key_timeout 5)
  set(fpta6_index_primary_timeout 60)
  set(fpta6_index_secondary_timeout 300)
  set(fpta7_cursor_primary_timeout 120)
  set(fpta7_cursor_secondary_unique_timeout 600)
  set(fpta7_cursor_secondary_withdups_timeout 600)
  set(fpta8_composite 120)
  set(fpta9_crud_timeout 120)
endif()

add_executable(fpta_c_mode c_mode.c)
target_link_libraries(fpta_c_mode fpta)

add_ut(fpta0_corny TIMEOUT ${fpta_small_timeout} SOURCE 0corny.cxx LIBRARY fptu fpta testutils)
add_ut(fpta1_open TIMEOUT ${fpta_small_timeout} SOURCE 1open.cxx LIBRARY fptu fpta testutils)
add_ut(fpta2_schema TIMEOUT ${fpta_small_timeout} SOURCE 2schema.cxx LIBRARY fptu fpta testutils)
add_ut(fpta3_smoke TIMEOUT ${fpta3_smoke_timeout} SOURCE 3smoke.cxx LIBRARY fptu fpta testutils)
add_ut(fpta4_data TIMEOUT ${fpta_small_timeout} SOURCE 4data.cxx LIBRARY fptu fpta testutils)

add_ut(fpta5_key TIMEOUT ${fpta5_key_timeout} SOURCE 5key.cxx LIBRARY fptu fpta testutils)
add_ut(fpta6_index_primary TIMEOUT ${fpta6_index_primary_timeout} SOURCE 6index_primary.cxx LIBRARY fptu fpta testutils)
add_ut(fpta6_index_secondary TIMEOUT ${fpta6_index_secondary_timeout} SOURCE 6index_secondary.cxx LIBRARY fptu fpta testutils)
add_ut(fpta7_cursor_primary TIMEOUT ${fpta7_cursor_primary_timeout} SOURCE 7cursor_primary.cxx LIBRARY fptu fpta testutils)
add_ut(fpta7_cursor_secondary_unique TIMEOUT ${fpta7_cursor_secondary_unique_timeout} SOURCE 7cursor_secondary_unique.cxx cursor_secondary.hpp LIBRARY fptu fpta testutils)
add_ut(fpta7_cursor_secondary_withdups TIMEOUT ${fpta7_cursor_secondary_withdups_timeout} SOURCE 7cursor_secondary_withdups.cxx cursor_secondary.hpp LIBRARY fptu fpta testutils)
add_ut(fpta8_composite TIMEOUT ${fpta8_composite} SOURCE 8composite.cxx LIBRARY fptu fpta testutils)
add_ut(fpta9_crud TIMEOUT ${fpta9_crud_timeout} SOURCE 9crud.cxx LIBRARY fptu fpta testutils)
