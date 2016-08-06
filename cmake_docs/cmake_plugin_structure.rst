project( **embed )

if (USE_**)
  ADD_PLUGIN(**embed PACKAGES ** OPTION MAKE_**EMBED MINVERSION 1.0 MAXVERSION 2.0)
  if ( MAKE_**EMBED )
    set ( SRCS
          **.cpp
        )

    include_directories (
         "${**_INCLUDE_DIR}"
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../rtl/include
         ./../../common/deftype
         ./../../system/jlib
       )

    ADD_DEFINITIONS( -D_USRDLL -D**EMBED_EXPORTS )

    HPCC_ADD_LIBRARY( **embed SHARED ${SRCS} )
    if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
      message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    else()
      set_target_properties( **embed PROPERTIES NO_SONAME 1 )
    endif()

    install ( TARGETS **embed DESTINATION plugins )
    target_link_libraries ( **embed ${**_LIBRARY} )

    target_link_libraries ( **embed
        eclrtl
        jlib
        )
  endif()
endif()

install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/**.ecllib DESTINATION plugins COMPONENT Runtime)
