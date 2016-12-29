add_library(fpta STATIC
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
)

target_link_libraries(fpta
    fptu mdbx t1ha ${CMAKE_THREAD_LIBS_INIT}
)
