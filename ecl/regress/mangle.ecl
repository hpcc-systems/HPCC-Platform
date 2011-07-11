/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

loadxml('<xml/>');

#declare(s1,s2)

#set(s1,'1$F')
#set(s2,#mangle(%'s1'%))

r1 := %'s2'%;

#set(s1,#demangle(%'s2'%))
r2 := %'s1'%;

#set(s1,'A1_F')
#set(s2,#mangle(%'s1'%))

r3 := %'s2'%;

#set(s1,#demangle(%'s2'%))
r4 := %'s1'%;
