add_subdirectory(libfptu/src)
add_subdirectory(libfptu/test)

###############################################################################

add_library(t1ha STATIC
  t1ha/src/t1ha0.c
  t1ha/src/t1ha1.c
  )

target_compile_definitions(t1ha PRIVATE T1HA_USE_FAST_ONESHOT_READ=$<BOOL:${T1HA_USE_FAST_ONESHOT_READ}>)

if(CC_HAS_ARCH_NATIVE)
  target_compile_options(t1ha PRIVATE "-march=native")
endif()

set_target_properties(t1ha PROPERTIES
  PROJECT_LABEL "Fast Positive Hash"
  #VERSION
  PUBLIC_HEADER "t1ha/t1ha.h"
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  )

###############################################################################

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(LIBMDBX_OSAL windows)
else()
  set(LIBMDBX_OSAL posix)
endif()

add_library(mdbx STATIC
  libmdbx/mdbx.h
  libmdbx/src/mdbx.c
  libmdbx/src/osal.c
  libmdbx/src/lck-${LIBMDBX_OSAL}.c
  libmdbx/src/version.c
  )

target_compile_definitions(mdbx PRIVATE MDBX_DEBUG=$<CONFIG:Debug> $<$<BOOL:${ENABLE_VALGRIND}>:ENABLE_VALGRIND>)
target_link_libraries(mdbx ${CMAKE_THREAD_LIBS_INIT})

if(UNIX AND NOT APPLE)
  target_link_libraries(mdbx rt)
endif()

set_target_properties(mdbx PROPERTIES
  PROJECT_LABEL "Fast Memory-Mapped key-value engine"
  #VERSION
  PUBLIC_HEADER "libmdbx/mdbx.h"
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  )

install(TARGETS t1ha mdbx
  ARCHIVE DESTINATION lib/static COMPONENT devel
  PUBLIC_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
  PRIVATE_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
  )

###############################################################################

set(MDBX_TOOLS_COMMON libmdbx/mdbx.h)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  list(APPEND MDBX_TOOLS_COMMON libmdbx/src/tools/wingetopt.h libmdbx/src/tools/wingetopt.c)
endif()

add_executable(mdbx_chk ${MDBX_TOOLS_COMMON} libmdbx/src/tools/mdbx_chk.c)
target_link_libraries(mdbx_chk mdbx)

add_executable(mdbx_stat ${MDBX_TOOLS_COMMON} libmdbx/src/tools/mdbx_stat.c)
target_link_libraries(mdbx_stat mdbx)

add_executable(mdbx_copy ${MDBX_TOOLS_COMMON} libmdbx/src/tools/mdbx_copy.c)
target_link_libraries(mdbx_copy mdbx)

add_executable(mdbx_dump ${MDBX_TOOLS_COMMON} libmdbx/src/tools/mdbx_dump.c)
target_link_libraries(mdbx_dump mdbx)

add_executable(mdbx_load ${MDBX_TOOLS_COMMON} libmdbx/src/tools/mdbx_load.c)
target_link_libraries(mdbx_load mdbx)

set_target_properties(mdbx_chk mdbx_stat mdbx_copy mdbx_dump mdbx_load PROPERTIES
  PROJECT_LABEL "MDBX's tools"
  #VERSION
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  )

install(TARGETS mdbx_chk mdbx_stat mdbx_copy mdbx_dump mdbx_load
  RUNTIME DESTINATION bin COMPONENT runtime
  )
