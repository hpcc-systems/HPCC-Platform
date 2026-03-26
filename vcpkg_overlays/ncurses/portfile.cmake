if(VCPKG_TARGET_IS_OSX)
    set(ncurses_system_prefixes "")
    if(DEFINED ENV{VCPKG_NCURSES_SYSTEM_PREFIX} AND NOT "$ENV{VCPKG_NCURSES_SYSTEM_PREFIX}" STREQUAL "")
        list(APPEND ncurses_system_prefixes "$ENV{VCPKG_NCURSES_SYSTEM_PREFIX}")
    endif()
    list(APPEND ncurses_system_prefixes "/opt/local" "/opt/homebrew" "/usr/local")

    set(ncurses_system_prefix "")
    foreach(prefix IN LISTS ncurses_system_prefixes)
        if(EXISTS "${prefix}/lib/libncursesw.a" OR EXISTS "${prefix}/lib/libncursesw.dylib")
            set(ncurses_system_prefix "${prefix}")
            break()
        endif()
    endforeach()

    if(ncurses_system_prefix)
        message(STATUS "Using system ncurses from ${ncurses_system_prefix}")

        file(MAKE_DIRECTORY
            "${CURRENT_PACKAGES_DIR}/include/ncursesw"
            "${CURRENT_PACKAGES_DIR}/lib"
            "${CURRENT_PACKAGES_DIR}/tools/${PORT}/bin"
            "${CURRENT_PACKAGES_DIR}/share/${PORT}"
        )

        foreach(header curses.h cursesw.h form.h menu.h ncurses.h ncurses_dll.h panel.h term.h termcap.h unctrl.h)
            if(EXISTS "${ncurses_system_prefix}/include/${header}")
                file(COPY "${ncurses_system_prefix}/include/${header}" DESTINATION "${CURRENT_PACKAGES_DIR}/include/ncursesw")
            endif()
        endforeach()

        if(VCPKG_LIBRARY_LINKAGE STREQUAL "dynamic")
            file(GLOB dylibs
                "${ncurses_system_prefix}/lib/libncursesw*.dylib"
                "${ncurses_system_prefix}/lib/libformw*.dylib"
                "${ncurses_system_prefix}/lib/libmenuw*.dylib"
                "${ncurses_system_prefix}/lib/libpanelw*.dylib"
            )
            if(dylibs)
                file(COPY ${dylibs} DESTINATION "${CURRENT_PACKAGES_DIR}/lib")
            endif()
        else()
            foreach(lib libncursesw.a libformw.a libmenuw.a libpanelw.a)
                if(EXISTS "${ncurses_system_prefix}/lib/${lib}")
                    file(COPY "${ncurses_system_prefix}/lib/${lib}" DESTINATION "${CURRENT_PACKAGES_DIR}/lib")
                endif()
            endforeach()
        endif()

        foreach(tool ncursesw6-config ncurses6-config)
            if(EXISTS "${ncurses_system_prefix}/bin/${tool}")
                file(COPY "${ncurses_system_prefix}/bin/${tool}" DESTINATION "${CURRENT_PACKAGES_DIR}/tools/${PORT}/bin")
            endif()
        endforeach()

        if(EXISTS "${ncurses_system_prefix}/lib/pkgconfig")
            file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
            file(GLOB pcs "${ncurses_system_prefix}/lib/pkgconfig/*ncurses*.pc" "${ncurses_system_prefix}/lib/pkgconfig/*form*.pc" "${ncurses_system_prefix}/lib/pkgconfig/*menu*.pc" "${ncurses_system_prefix}/lib/pkgconfig/*panel*.pc")
            if(pcs)
                file(COPY ${pcs} DESTINATION "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
            endif()
        endif()

        file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
        if(EXISTS "${ncurses_system_prefix}/share/doc/ncurses/COPYING")
            vcpkg_install_copyright(FILE_LIST "${ncurses_system_prefix}/share/doc/ncurses/COPYING")
        else()
            file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright" "ncurses is provided by system package manager.\n")
        endif()
        return()
    endif()
endif()

vcpkg_download_distfile(
    ARCHIVE_PATH
    URLS
        "https://invisible-mirror.net/archives/ncurses/ncurses-${VERSION}.tar.gz"
        "ftp://ftp.invisible-island.net/ncurses/ncurses-${VERSION}.tar.gz"
        "https://ftp.gnu.org/gnu/ncurses/ncurses-${VERSION}.tar.gz"
    FILENAME "ncurses-${VERSION}.tgz"
    SHA512 1c2efff87a82a57e57b0c60023c87bae93f6718114c8f9dc010d4c21119a2f7576d0225dab5f0a227c2cfc6fb6bdbd62728e407f35fce5bf351bb50cf9e0fd34
)

vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE_PATH}"
)

vcpkg_list(SET OPTIONS)

if(VCPKG_LIBRARY_LINKAGE STREQUAL "dynamic")
    list(APPEND OPTIONS
        --with-cxx-shared
        --with-shared    # "lib model"
        --without-normal # "lib model"
    )
endif()

if(NOT VCPKG_TARGET_IS_MINGW)
    list(APPEND OPTIONS
        --enable-mixed-case
    )
endif()

if(VCPKG_TARGET_IS_MINGW)
    list(APPEND OPTIONS
        --disable-home-terminfo
        --enable-term-driver
        --disable-termcap
    )
endif()

vcpkg_cmake_get_vars(cmake_vars_file)
include("${cmake_vars_file}")

# There are compilation errors on gcc 15. adding `-std=c17` to CFLAGS for workaround.
# ref: https://gitlab.archlinux.org/archlinux/packaging/packages/ncurses/-/issues/3
if(VCPKG_DETECTED_CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND VCPKG_DETECTED_CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 15)
    set(ENV{CFLAGS} "$ENV{CFLAGS} -std=c17")
endif()

vcpkg_configure_make(
    SOURCE_PATH "${SOURCE_PATH}"
    CONFIGURE_ENVIRONMENT_VARIABLES CFLAGS
    DETERMINE_BUILD_TRIPLET
    NO_ADDITIONAL_PATHS
    OPTIONS
        ${OPTIONS}
        --disable-db-install
        --enable-pc-files
        --without-ada
        --without-debug # "lib model"
        --without-manpages
        --without-progs
        --without-tack
        --without-tests
        --with-pkg-config-libdir=libdir
)
vcpkg_install_make()

vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/bin")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/bin")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")

file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
