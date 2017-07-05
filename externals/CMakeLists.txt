add_subdirectory(libfptu/src)
add_subdirectory(libfptu/test)

add_library(t1ha STATIC
  t1ha/src/t1ha0.c
  t1ha/src/t1ha1.c
  )

if(CC_HAS_ARCH_NATIVE)
  target_compile_options(t1ha PRIVATE "-march=native")
endif()

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

target_compile_definitions(mdbx PRIVATE $<$<CONFIG:Debug>:MDBX_DEBUG=1> $<$<BOOL:${ENABLE_VALGRIND}>:ENABLE_VALGRIND>)
target_link_libraries(mdbx ${CMAKE_THREAD_LIBS_INIT})

if(UNIX AND NOT APPLE)
  target_link_libraries(mdbx rt)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  message(WARNING "Now skiping MDBX-tools for Windows (TODO)")
else()

  add_executable(mdbx_chk libmdbx/src/tools/mdbx_chk.c)
  target_link_libraries(mdbx_chk mdbx)

  add_executable(mdbx_stat libmdbx/src/tools/mdbx_stat.c)
  target_link_libraries(mdbx_stat mdbx)

  add_executable(mdbx_copy libmdbx/src/tools/mdbx_copy.c)
  target_link_libraries(mdbx_copy mdbx)

  add_executable(mdbx_dump libmdbx/src/tools/mdbx_dump.c)
  target_link_libraries(mdbx_dump mdbx)

  add_executable(mdbx_load libmdbx/src/tools/mdbx_load.c)
  target_link_libraries(mdbx_load mdbx)

endif()
