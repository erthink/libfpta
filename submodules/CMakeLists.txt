add_subdirectory(libfptu/src)
add_subdirectory(libfptu/test)

add_library(t1ha STATIC
   t1ha/t1ha.c
)

add_library(mdbx STATIC
   libmdbx/mdbx.h
   libmdbx/lmdb.h
   libmdbx/mdbx.c
)

set_target_properties(mdbx PROPERTIES COMPILE_FLAGS "-DNDEBUG")

add_executable(mdbx_chk libmdbx/mdb_chk.c)
target_link_libraries(mdbx_chk mdbx)

add_executable(mdbx_stat libmdbx/mdb_stat.c)
target_link_libraries(mdbx_stat mdbx)

add_executable(mdbx_copy libmdbx/mdb_copy.c)
target_link_libraries(mdbx_copy mdbx)

add_executable(mdbx_dump libmdbx/mdb_dump.c)
target_link_libraries(mdbx_dump mdbx)

add_executable(mdbx_load libmdbx/mdb_load.c)
target_link_libraries(mdbx_load mdbx)
