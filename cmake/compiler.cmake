#
# Check if the same compile family is used for both C and CXX
#
if(NOT (CMAKE_C_COMPILER_ID STREQUAL CMAKE_CXX_COMPILER_ID))
  message(WARNING "CMAKE_C_COMPILER_ID (${CMAKE_C_COMPILER_ID}) is different "
    "from CMAKE_CXX_COMPILER_ID (${CMAKE_CXX_COMPILER_ID})."
    "The final binary may be unusable.")
endif()

# We support building with Clang and gcc. First check
# what we're using for build.
#
if(CMAKE_C_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_COMPILER_IS_CLANG  ON)
  set(CMAKE_COMPILER_IS_GNUCC  OFF)
  set(CMAKE_COMPILER_IS_GNUCXX OFF)
endif()

# cmake 2.8.9 and earlier doesn't support CMAKE_CXX_COMPILER_VERSION
if(NOT CMAKE_CXX_COMPILER_VERSION AND (CMAKE_COMPILER_IS_CLANG OR CMAKE_COMPILER_IS_GNUCXX))
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion
    OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION)
endif()
if(NOT CMAKE_C_COMPILER_VERSION AND (CMAKE_COMPILER_IS_CLANG OR CMAKE_COMPILER_IS_GNUC))
  execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion
    OUTPUT_VARIABLE CMAKE_C_COMPILER_VERSION)
endif()

#
# Hard coding the compiler version is ugly from cmake POV, but
# at least gives user a friendly error message. The most critical
# demand for C++ compiler is support of C++11 lambdas, added
# only in version 4.5 https://gcc.gnu.org/projects/cxx0x.html
#
if(CMAKE_COMPILER_IS_GNUCC)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.5)
    message(FATAL_ERROR "
      Your GCC version is ${CMAKE_CXX_COMPILER_VERSION}, please update")
  endif()
endif()

#
# Check supported standards
#
if((NOT HAVE_STD_C11 AND NOT HAVE_STD_C99 AND NOT HAVE_STD_GNU99) OR
    (NOT HAVE_STD_CXX11 AND NOT HAVE_STD_GNUXX0X))
  set(CMAKE_REQUIRED_FLAGS "-std=c11")
  check_c_source_compiles("
    #if __STDC_VERSION__ < 201112L
    #   error C11 not available
    #endif
    /*
    * FreeBSD 10 ctype.h header fail to compile on gcc4.8 in c11 mode.
    * Make sure we aren't affected.
    */
    #include <ctype.h>
    int main(void) { return 0; }
    " HAVE_STD_C11)

  set(CMAKE_REQUIRED_FLAGS "-std=c99")
  check_c_source_compiles("
    #if (__STDC_VERSION__ < 199901L) && (_MSC_FULL_VER < 180040629)
    #   error C99 not available
    #endif
    int main(void) { return 0; }
    " HAVE_STD_C99)

  set(CMAKE_REQUIRED_FLAGS "-std=gnu99")
  check_c_source_compiles("
    #if __STDC_VERSION__ < 199901L
    #   error C99 not available
    #endif
    int main(void) { return 0; }
    " HAVE_STD_GNU99)

  set(CMAKE_REQUIRED_FLAGS "-std=c++11")
  check_cxx_source_compiles("
    #if (__cplusplus < 201103L) && (_MSC_FULL_VER < 180040629)
    #   error C++11 not available
    #endif
    int main(void) { return 0; }
    " HAVE_STD_CXX11)
  set(CMAKE_REQUIRED_FLAGS "-std=gnu++0x")

  check_cxx_source_compiles("
    #if __cplusplus < 201103L && !defined(__GXX_EXPERIMENTAL_CXX0X__)
    #   error GNU C++0x not available
    #endif
    int main(void) { return 0; }
    " HAVE_STD_GNUXX0X)
  set(CMAKE_REQUIRED_FLAGS "")
endif()
if((NOT HAVE_STD_C11 AND NOT HAVE_STD_C99 AND NOT HAVE_STD_GNU99) OR
    (NOT HAVE_STD_CXX11 AND NOT HAVE_STD_GNUXX0X))
  message(FATAL_ERROR
    "${CMAKE_C_COMPILER} should support -std=c11 or -std=c99. "
    "${CMAKE_CXX_COMPILER} should support -std=c++11 or -std=gnu++0x. "
    "Please consider upgrade to gcc 4.5+ or clang 3.2+.")
endif()

if(MSVC)
  check_c_compiler_flag("/WX" CC_HAS_WERROR)
else()
  #
  # GCC started to warn for unused result starting from 4.2, and
  # this is when it introduced -Wno-unused-result
  # GCC can also be built on top of llvm runtime (on mac).
  #
  check_c_compiler_flag("-Wno-unused-const-variable" CC_HAS_WNO_UNUSED_CONST_VARIABLE)
  check_c_compiler_flag("-Wno-unused-result" CC_HAS_WNO_UNUSED_RESULT)
  check_c_compiler_flag("-Wno-unused-value" CC_HAS_WNO_UNUSED_VALUE)
  check_c_compiler_flag("-Wno-unused-function" CC_HAS_WNO_UNUSED_FUNCTION)
  check_c_compiler_flag("-fno-strict-aliasing" CC_HAS_FNO_STRICT_ALIASING)
  check_c_compiler_flag("-Wno-comment" CC_HAS_WNO_COMMENT)
  check_c_compiler_flag("-Wno-parentheses" CC_HAS_WNO_PARENTHESES)
  check_c_compiler_flag("-Wno-parentheses-equality" CC_HAS_WNO_PARENTHESES_EQUALITY)
  check_c_compiler_flag("-Wno-undefined-inline" CC_HAS_WNO_UNDEFINED_INLINE)
  check_c_compiler_flag("-Wno-dangling-else" CC_HAS_WNO_DANGLING_ELSE)
  check_c_compiler_flag("-Wno-tautological-compare" CC_HAS_WNO_TAUTOLOGICAL_COMPARE)
  check_c_compiler_flag("-Wno-misleading-indentation" CC_HAS_WNO_MISLEADING_INDENTATION)

  check_c_compiler_flag("-Wno-unknown-pragmas" CC_HAS_WNO_UNKNOWN_PRAGMAS)
  check_c_compiler_flag("-Wextra" CC_HAS_WEXTRA)
  check_c_compiler_flag("-Werror" CC_HAS_WERROR)
  check_c_compiler_flag("-fexceptions" CC_HAS_FEXCEPTIONS)
  check_c_compiler_flag("-funwind-tables" CC_HAS_FUNWIND_TABLES)
  check_c_compiler_flag("-fno-omit-frame-pointer" CC_HAS_FNO_OMIT_FRAME_POINTER)
  check_c_compiler_flag("-fno-stack-protector" CC_HAS_FNO_STACK_PROTECTOR)
  check_c_compiler_flag("-fno-common" CC_HAS_FNO_COMMON)
  check_c_compiler_flag("-Wno-strict-aliasing" CC_HAS_WNO_STRICT_ALIASING)
  check_c_compiler_flag("-ggdb" CC_HAS_GGDB)
  check_c_compiler_flag("-fvisibility=hidden" CC_HAS_VISIBILITY)
  check_c_compiler_flag("-march=native" CC_HAS_ARCH_NATIVE)
  check_c_compiler_flag("-Wall" CC_HAS_WALL)

  #
  # Check for an omp support
  set(CMAKE_REQUIRED_FLAGS "-fopenmp -Werror")
  check_cxx_source_compiles("int main(void) {
    #pragma omp parallel
    return 0;
    }" HAVE_OPENMP)
  set(CMAKE_REQUIRED_FLAGS "")
endif()

#
# Check for LTO support by GCC
if(CMAKE_COMPILER_IS_GNUCC)
  unset(gcc_collect)
  unset(gcc_lto_wrapper)

  if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.7)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -v
      OUTPUT_VARIABLE gcc_info_v ERROR_VARIABLE gcc_info_v)

    string(REGEX MATCH "^(.+\nCOLLECT_GCC=)([^ \n]+)(\n.+)$" gcc_collect_valid ${gcc_info_v})
    if(gcc_collect_valid)
      string(REGEX REPLACE "^(.+\nCOLLECT_GCC=)([^ \n]+)(\n.+)$" "\\2" gcc_collect ${gcc_info_v})
    endif()

    string(REGEX MATCH "^(.+\nCOLLECT_LTO_WRAPPER=)([^ \n]+/lto-wrapper)(\n.+)$" gcc_lto_wrapper_valid ${gcc_info_v})
    if(gcc_lto_wrapper_valid)
      string(REGEX REPLACE "^(.+\nCOLLECT_LTO_WRAPPER=)([^ \n]+/lto-wrapper)(\n.+)$" "\\2" gcc_lto_wrapper ${gcc_info_v})
    endif()

    set(gcc_suffix "")
    if(gcc_collect_valid AND gcc_collect)
      string(REGEX MATCH "^(.*cc)(-.+)$" gcc_suffix_valid ${gcc_collect})
      if(gcc_suffix_valid)
        string(REGEX MATCH "^(.*cc)(-.+)$" "\\2" gcc_suffix ${gcc_collect})
      endif()
    endif()

    get_filename_component(gcc_dir ${CMAKE_C_COMPILER} DIRECTORY)
    if(NOT CMAKE_GCC_AR)
      find_program(CMAKE_GCC_AR NAMES gcc${gcc_suffix}-ar gcc-ar${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_GCC_NM)
      find_program(CMAKE_GCC_NM NAMES gcc${gcc_suffix}-nm gcc-nm${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_GCC_RANLIB)
      find_program(CMAKE_GCC_RANLIB NAMES gcc${gcc_suffix}-ranlib gcc-ranlib${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()

    unset(gcc_dir)
    unset(gcc_suffix_valid)
    unset(gcc_suffix)
    unset(gcc_lto_wrapper_valid)
    unset(gcc_collect_valid)
    unset(gcc_collect)
    unset(gcc_info_v)
  endif()

  if(CMAKE_GCC_AR AND CMAKE_GCC_NM AND CMAKE_GCC_RANLIB AND gcc_lto_wrapper)
    message(STATUS "Found GCC's LTO toolset: ${gcc_lto_wrapper}, ${CMAKE_GCC_AR}, ${CMAKE_GCC_RANLIB}")
    set(GCC_LTO_CFLAGS "-flto -fno-fat-lto-objects -fuse-linker-plugin")
    set(GCC_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by GCC is available")
  else()
    set(GCC_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by GCC is NOT available")
  endif()
  unset(gcc_lto_wrapper)
endif()

#
# check for LTO by MSVC
if(MSVC)
  if(NOT MSVC_VERSION LESS 1600)
    set(MSVC_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by MSVC is available")
  else()
    set(MSVC_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by MSVC is NOT available")
  endif()
endif()

#
# Check for LTO support by CLANG
if(CMAKE_COMPILER_IS_CLANG)
  if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.5)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -print-search-dirs
      OUTPUT_VARIABLE clang_search_dirs)

    unset(clang_bindir)
    unset(clang_libdir)
    string(REGEX MATCH "^(.*programs: =)([^:]*:)*([^:]+/llvm[-.0-9]+/bin[^:]*)(:[^:]*)*(\n.+)$" clang_bindir_valid ${clang_search_dirs})
    if(clang_bindir_valid)
      string(REGEX REPLACE "^(.*programs: =)([^:]*:)*([^:]+/llvm[-.0-9]+/bin[^:]*)(:[^:]*)*(\n.+)$" "\\3" clang_bindir ${clang_search_dirs})
      get_filename_component(clang_libdir "${clang_bindir}/../lib" REALPATH)
      if(clang_libdir)
        message(STATUS "Found CLANG/LLVM directories: ${clang_bindir}, ${clang_libdir}")
      endif()
    endif()

    if(NOT (clang_bindir AND clang_libdir))
      message(STATUS "Could NOT find CLANG/LLVM directories (bin and/or lib).")
    endif()

    if(NOT CMAKE_CLANG_LD AND clang_bindir)
      find_program(CMAKE_CLANG_LD NAMES llvm-link link llvm-ld ld PATHS ${clang_bindir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_AR AND clang_bindir)
      find_program(CMAKE_CLANG_AR NAMES llvm-ar ar PATHS ${clang_bindir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_NM AND clang_bindir)
      find_program(CMAKE_CLANG_NM NAMES llvm-nm nm PATHS ${clang_bindir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_RANLIB AND clang_bindir)
      find_program(CMAKE_CLANG_RANLIB NAMES llvm-ranlib ranlib PATHS ${clang_bindir} NO_DEFAULT_PATH)
    endif()

    set(clang_lto_plugin_name "LLVMgold${CMAKE_SHARED_LIBRARY_SUFFIX}")
    if(NOT CMAKE_LD_GOLD AND clang_bindir)
      find_program(CMAKE_LD_GOLD NAMES ld.gold PATHS)
    endif()
    if(NOT CLANG_LTO_PLUGIN AND clang_libdir)
      find_file(CLANG_LTO_PLUGIN ${clang_lto_plugin_name} PATH ${clang_libdir} NO_DEFAULT_PATH)
    endif()
    if(CLANG_LTO_PLUGIN)
      message(STATUS "Found CLANG/LLVM's plugin for LTO: ${CLANG_LTO_PLUGIN}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM's plugin (${clang_lto_plugin_name}) for LTO.")
    endif()

    if(CMAKE_CLANG_LD AND CMAKE_CLANG_AR AND CMAKE_CLANG_NM AND CMAKE_CLANG_RANLIB)
      message(STATUS "Found CLANG/LLVM's binutils for LTO: ${CMAKE_CLANG_AR}, ${CMAKE_CLANG_RANLIB}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM's binutils (ar, ranlib, nm) for LTO.")
    endif()

    unset(clang_lto_plugin_name)
    unset(clang_libdir)
    unset(clang_bindir_valid)
    unset(clang_bindir)
    unset(clang_search_dirs)
  endif()

  if((CLANG_LTO_PLUGIN AND CMAKE_LD_GOLD) AND
      (CMAKE_CLANG_LD AND CMAKE_CLANG_AR AND CMAKE_CLANG_NM AND CMAKE_CLANG_RANLIB))
    set(CLANG_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by CLANG/LLVM is available")
  else()
    set(CLANG_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by CLANG/LLVM is NOT available")
  endif()
endif()

#
# Perform build type specific configuration.
option(ENABLE_BACKTRACE "Enable output of fiber backtrace information in 'show
  fiber' administrative command. Only works on x86 architectures, if compiled
  with gcc. If GNU binutils and binutils-dev libraries are installed, backtrace
  is output with resolved function (symbol) names. Otherwise only frame
  addresses are printed." ${CMAKE_COMPILER_IS_GNUCC})

set(HAVE_BFD False)
if(ENABLE_BACKTRACE)
  if(NOT CMAKE_COMPILER_IS_GNUCC)
    # We only know this option to work with gcc
    message(FATAL_ERROR "ENABLE_BACKTRACE option is set but the system
      is not x86 based (${CMAKE_SYSTEM_PROCESSOR}) or the compiler
      is not GNU GCC (${CMAKE_C_COMPILER}).")
  endif()
  # Use GNU bfd if present.
  find_library(BFD_LIBRARY NAMES libbfd.a)
  if(BFD_LIBRARY)
    check_library_exists(${BFD_LIBRARY} bfd_init "" HAVE_BFD_LIB)
  endif()
  find_library(IBERTY_LIBRARY NAMES libiberty.a)
  if(IBERTY_LIBRARY)
    check_library_exists(${IBERTY_LIBRARY} cplus_demangle "" HAVE_IBERTY_LIB)
  endif()
  set(CMAKE_REQUIRED_DEFINITIONS -DPACKAGE=${PACKAGE} -DPACKAGE_VERSION=${PACKAGE_VERSION})
  check_include_files(bfd.h HAVE_BFD_H)
  set(CMAKE_REQUIRED_DEFINITIONS)
  find_package(ZLIB)
  if(HAVE_BFD_LIB AND HAVE_BFD_H AND HAVE_IBERTY_LIB AND ZLIB_FOUND)
    set(HAVE_BFD ON)
    set(BFD_LIBRARIES ${BFD_LIBRARY} ${IBERTY_LIBRARY} ${ZLIB_LIBRARIES})
    find_package_message(BFD_LIBRARIES "Found libbfd and dependencies"
      ${BFD_LIBRARIES})
    if(TARGET_OS_FREEBSD AND NOT TARGET_OS_DEBIAN_FREEBSD)
      set(BFD_LIBRARIES ${BFD_LIBRARIES} iconv)
    endif()
  endif()
endif()

# In C a global variable without a storage specifier (static/extern) and
# without an initialiser is called a ’tentative definition’. The
# language permits multiple tentative definitions in the single
# translation unit; i.e. int foo; int foo; is perfectly ok. GNU
# toolchain goes even further, allowing multiple tentative definitions
# in *different* translation units. Internally, variables introduced via
# tentative definitions are implemented as ‘common’ symbols. Linker
# permits multiple definitions if they are common symbols, and it picks
# one arbitrarily for inclusion in the binary being linked.
#
# -fno-common forces GNU toolchain to behave in a more
# standard-conformant way in respect to tentative definitions and it
# prevents common symbols generation. Since we are a cross-platform
# project it really makes sense. There are toolchains that don’t
# implement GNU style handling of the tentative definitions and there
# are platforms lacking proper support for common symbols (osx).
#

macro(setup_compile_flags)
  # LY: save initial C/CXX flags
  if(NOT INITIAL_CMAKE_FLAGS_SAVED)
    set(INITIAL_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_C_FLAGS ${CMAKE_C_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_SHARED_LINKER_FLAGS ${CMAKE_SHARED_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_STATIC_LINKER_FLAGS ${CMAKE_STATIC_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_MODULE_LINKER_FLAGS ${CMAKE_MODULE_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_FLAGS_SAVED TRUE CACHE INTERNAL "State of initial CMake's flags" FORCE)
  endif()

  # LY: reset C/CXX flags
  set(CXX_FLAGS ${INITIAL_CMAKE_CXX_FLAGS})
  set(C_FLAGS ${INITIAL_CMAKE_C_FLAGS})
  set(EXE_LINKER_FLAGS ${INITIAL_CMAKE_EXE_LINKER_FLAGS})
  set(SHARED_LINKER_FLAGS ${INITIAL_CMAKE_SHARED_LINKER_FLAGS})
  set(STATIC_LINKER_FLAGS ${INITIAL_CMAKE_STATIC_LINKER_FLAGS})
  set(MODULE_LINKER_FLAGS ${INITIAL_CMAKE_MODULE_LINKER_FLAGS})

  if(CMAKE_COMPILER_IS_GNUCC AND GCC_LTO_ENABLED)
    add_compile_flags("C;CXX" ${GCC_LTO_CFLAGS})
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm -fwhole-program")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
      # Pass the same optimization flags to the linker
      set(compile_flags "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}")
      set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} ${compile_flags}")
      set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} ${compile_flags}")
      set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} ${compile_flags}")
      unset(compile_flags)
    else()
      add_compile_flags("CXX" "-flto-odr-type-merging")
    endif()
  endif()

  if(MSVC AND MSVC_LTO_ENABLED)
    add_compile_flags("C;CXX" "/GL")
    foreach(linkmode IN ITEMS EXE SHARED STATIC MODULE)
      set(${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS} /LTCG")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL:NO *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL:YES *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(STRIP "${${linkmode}_LINKER_FLAGS}" ${linkmode}_LINKER_FLAGS)
      foreach(config IN LISTS CMAKE_CONFIGURATION_TYPES ITEMS Release MinSizeRel RelWithDebInfo Debug)
        string(TOUPPER "${config}" config_uppercase)
        if(DEFINED "CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL:NO *)(.*)$" "\\1\\3" altered_flags "${CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL:YES *)(.*)$" "\\1\\3" altered_flags "${altered_flags}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL *)(.*)$" "\\1\\3" altered_flags "${altered_flags}")
          string(STRIP "${altered_flags}" altered_flags)
          if(NOT "${altered_flags}" STREQUAL "${CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}}")
            set(CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase} "${altered_flags}" CACHE STRING "Altered: '/INCREMENTAL' removed for LTO" FORCE)
          endif()
        endif()
      endforeach(config)
    endforeach(linkmode)
    unset(linkmode)

    foreach(config IN LISTS CMAKE_CONFIGURATION_TYPES ITEMS Release MinSizeRel RelWithDebInfo)
      foreach(lang IN ITEMS C CXX)
        string(TOUPPER "${config}" config_uppercase)
        if(DEFINED "CMAKE_${lang}_FLAGS_${config_uppercase}")
          string(REPLACE "/O2" "/Ox" altered_flags "${CMAKE_${lang}_FLAGS_${config_uppercase}}")
          if(NOT "${altered_flags}" STREQUAL "${CMAKE_${lang}_FLAGS_${config_uppercase}}")
            set(CMAKE_${lang}_FLAGS_${config_uppercase} "${altered_flags}" CACHE STRING "Altered: '/O2' replaced by '/Ox' for LTO" FORCE)
          endif()
        endif()
      endforeach(lang)
    endforeach(config)
    unset(altered_flags)
    unset(lang)
    unset(config)
  endif()

  if(CMAKE_COMPILER_IS_CLANG AND CLANG_LTO_ENABLED)
    add_compile_flags("C;CXX" "-flto")
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} -flto -fverbose-asm -fwhole-program")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} -flto -fverbose-asm")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} -flto -fverbose-asm")
  endif()

  if(CC_HAS_FNO_COMMON)
    add_compile_flags("C;CXX" "-fno-common")
  endif()

  if(CC_HAS_GGDB)
    add_compile_flags("C;CXX" "-ggdb")
  endif()

  if(CC_HAS_WNO_UNKNOWN_PRAGMAS AND NOT HAVE_OPENMP)
    add_compile_flags("C;CXX" -Wno-unknown-pragmas)
  endif()

  # We must set -fno-omit-frame-pointer here, since we rely
  # on frame pointer when getting a backtrace, and it must
  # be used consistently across all object files.
  # The same reasoning applies to -fno-stack-protector switch.
  if(ENABLE_BACKTRACE)
    if(CC_HAS_FNO_OMIT_FRAME_POINTER)
      add_compile_flags("C;CXX" "-fno-omit-frame-pointer")
    endif()
    if(CC_HAS_FNO_STACK_PROTECTOR)
      add_compile_flags("C;CXX" "-fno-stack-protector")
    endif()
  endif()

  # Set standard
  if(HAVE_STD_C11)
    add_compile_flags("C" "-std=c11")
  elseif(HAVE_STD_GNU99)
    add_compile_flags("C" "-std=gnu99")
  elseif(HAVE_STD_C99 AND NOT MSVC)
    add_compile_flags("C" "-std=c99")
  endif()

  if(HAVE_STD_CXX11 AND NOT MSVC)
    add_compile_flags("CXX" "-std=c++11")
  elseif(HAVE_STD_GNUXX0X)
    add_compile_flags("CXX" "-std=gnu++0x")
    add_definitions("-Doverride=")
  endif()

  if(MSVC)
    if(MSVC_VERSION EQUAL 1910)
      # LY: avoid /Wall for Visual Studio 2017, otherwise due a bug we could lost the control
      #     and get a lot of junk warnings from compiler's and SDK headers.
      add_compile_flags("C;CXX" "/W4")
    else()
      add_compile_flags("C;CXX" "/Wall")
    endif()
  elseif(CC_HAS_WALL)
    add_compile_flags("C;CXX" "-Wall")
  endif()
  if(CC_HAS_WEXTRA)
    add_compile_flags("C;CXX" "-Wextra")
  endif()

  if(CMAKE_COMPILER_IS_GNUCXX
      AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
    # G++ bug. http://gcc.gnu.org/bugzilla/show_bug.cgi?id=31488
    add_compile_flags("CXX" "-Wno-invalid-offsetof")
  endif()

  add_definitions("-D__STDC_FORMAT_MACROS=1")
  add_definitions("-D__STDC_LIMIT_MACROS=1")
  add_definitions("-D__STDC_CONSTANT_MACROS=1")
  add_definitions("-D_HAS_EXCEPTIONS=0")

  # Only add -Werror if it's a debug build, done by developers.
  # Release builds should not cause extra trouble.
  if(CC_HAS_WERROR AND (CI OR CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE STREQUAL "Debug"))
    if(MSVC)
      add_compile_flags("C;CXX" "/WX")
    elseif(HAVE_STD_C11 AND HAVE_STD_CXX11)
      add_compile_flags("C;CXX" "-Werror")
    endif()
  endif()

  if(HAVE_OPENMP)
    add_compile_flags("C;CXX" "-fopenmp")
  endif()

  if(ENABLE_GCOV)
    if(NOT HAVE_GCOV)
      message(FATAL_ERROR
        "ENABLE_GCOV option requested but gcov library is not found")
    endif()

    add_compile_flags("C;CXX" "-fprofile-arcs" "-ftest-coverage")
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    # add_library(gcov SHARED IMPORTED)
  endif()

  if(ENABLE_GPROF)
    add_compile_flags("C;CXX" "-pg")
  endif()

  # LY: push C/CXX flags into the cache
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS} CACHE STRING "Flags used by the C++ compiler during all build types" FORCE)
  set(CMAKE_C_FLAGS ${C_FLAGS} CACHE STRING "Flags used by the C compiler during all build types" FORCE)
  set(CMAKE_EXE_LINKER_FLAGS ${EXE_LINKER_FLAGS} CACHE STRING "Flags used by the linker" FORCE)
  set(CMAKE_SHARED_LINKER_FLAGS ${SHARED_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of dll's" FORCE)
  set(CMAKE_STATIC_LINKER_FLAGS ${STATIC_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of static libraries" FORCE)
  set(CMAKE_MODULE_LINKER_FLAGS ${MODULE_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of modules" FORCE)
  unset(CXX_FLAGS)
  unset(C_FLAGS)
  unset(EXE_LINKER_FLAGS)
  unset(SHARED_LINKER_FLAGS)
  unset(STATIC_LINKER_FLAGS)
  unset(MODULE_LINKER_FLAGS)
endmacro(setup_compile_flags)

if(CMAKE_COMPILER_IS_CLANG OR CMAKE_COMPILER_IS_GNUCC)
  set(HAVE_BUILTIN_CTZ 1)
  set(HAVE_BUILTIN_CTZLL 1)
  set(HAVE_BUILTIN_CLZ 1)
  set(HAVE_BUILTIN_CLZLL 1)
  set(HAVE_BUILTIN_POPCOUNT 1)
  set(HAVE_BUILTIN_POPCOUNTLL 1)
  set(HAVE_BUILTIN_BSWAP32 1)
  set(HAVE_BUILTIN_BSWAP64 1)
else()
  set(HAVE_BUILTIN_CTZ 0)
  set(HAVE_BUILTIN_CTZLL 0)
  set(HAVE_BUILTIN_CLZ 0)
  set(HAVE_BUILTIN_CLZLL 0)
  set(HAVE_BUILTIN_POPCOUNT 0)
  set(HAVE_BUILTIN_POPCOUNTLL 0)
  set(HAVE_BUILTIN_BSWAP32 0)
  set(HAVE_BUILTIN_BSWAP64 0)
  find_package_message(CC_BIT "Using slow implementation of bit operations"
    "${CMAKE_COMPILER_IS_CLANG}:${CMAKE_COMPILER_IS_GNUCC}")
endif()

if(NOT HAVE_BUILTIN_CTZ OR NOT HAVE_BUILTIN_CTZLL)
  # Check if -D_GNU_SOURCE has been defined and add this flag to
  # CMAKE_REQUIRED_DEFINITIONS in order to get check_prototype_definition work
  get_property(var DIRECTORY PROPERTY COMPILE_DEFINITIONS)
  list(FIND var "_GNU_SOURCE" var)
  if(NOT var EQUAL -1)
    set(CMAKE_REQUIRED_FLAGS "-Wno-error")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    check_c_source_compiles("#include <string.h>\n#include <strings.h>\nint main(void) { return ffsl(0L); }"
      HAVE_FFSL)
    check_c_source_compiles("#include <string.h>\n#include <strings.h>\nint main(void) { return ffsll(0UL); }"
      HAVE_FFSLL)
  endif()
endif()
