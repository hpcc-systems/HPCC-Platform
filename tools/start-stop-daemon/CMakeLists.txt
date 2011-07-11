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

project(start-stop-daemon)


find_package( StartStopDaemon )

option(FORCE-START-STOP-DAEMON "Force building of start-stop-daemon" OFF)


if ( STARTSTOPDAEMON_FOUND AND NOT FORCE-START-STOP-DAEMON)
set ( START-STOP-DAEMON OFF )
 message( "-- start-stop-daemon found on system.")
 message( "-- start-stop-daemon will not be built.")
else()
 set ( START-STOP-DAEMON ON )
 if( FORCE-START-STOP-DAEMON )
  message( "-- start-stop-daemon force build has been set to ON.")
 else()
  message( "-- start-stop-daemon not found on system.")
 endif()
 message( "-- start-stop-daemon will be Built.")
endif()




if( START-STOP-DAEMON )
        project(start-stop-daemon)

        set( SRC start-stop-daemon.c )
        set( HEADER macros.h )

        include_directories( ${CMAKE_CURRENT_SOURCE_DIR} )

        add_executable( start-stop-daemon ${SRC} ${HEADER})
    install ( TARGETS start-stop-daemon DESTINATION ${OSSDIR}/bin )
endif()



