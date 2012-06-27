################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

cmake_minimum_required(VERSION 2.8)
PROJECT(lib2)

if (APPLE)
    set(DYLIBS ${ICU_LIBRARIES})
    set(DYLIBS ${DYLIBS} ${BOOST_REGEX_LIBRARIES})
    set(DYLIBS ${DYLIBS} ${XALAN_LIBRARIES})
    set(DYLIBS ${DYLIBS} ${XERCES_LIBRARIES})
    
    foreach(dylib ${DYLIBS})
        get_filename_component(dylib_path ${dylib} REALPATH)
        get_filename_component(dylib_name_ext ${dylib_path} NAME)

        set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")

        #HACK HACK - Should be able resolve alias's to alias's correctly?
        string(REPLACE ".28.0.dylib" ".28.dylib" dylib_28_path "${dylib_path}")
        if (NOT "${dylib_28_path}" STREQUAL "${dylib_path}")
            set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_28_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")
        endif ()

        string(REPLACE ".48.1.dylib" ".48.dylib" dylib_48_path "${dylib_path}")
        if (NOT "${dylib_48_path}" STREQUAL "${dylib_path}")
            set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_48_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")
        endif ()

        install(PROGRAMS "${dylib_path}" DESTINATION "${OSSDIR}/lib2")
        install(CODE "
            file(GLOB files \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${OSSDIR}/bin/*\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${OSSDIR}/lib/*.dylib\")
            foreach(file \${files})
              ${fixupCommand}
            endforeach ()
        ")
    endforeach(dylib)
endif()
