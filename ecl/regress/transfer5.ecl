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

output((>string<)X'4142434445');    //ABCDE
output((>string3<)X'4142434445');   //ABC
output((>string4<)0x44434241);      //ABCD little endian
output((>string3<)0x44434241);      //ABC little endian
output((>string<)0x44434241);       //ABCDxxxx little endian



output((>integer4<)X'01020304');
output((>integer4<)X'010203040506');


output((>string3<)123456.78D);
output((>string<)123456.78D);
