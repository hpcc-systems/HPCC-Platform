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

loadxml('<__x_xr/>');

//Constant fold at generate time
#declare(constMyYear)
#SET(constMyYear,(unsigned4)(stringlib.getDateYYYYMMDD()[1..4]));
#STORED('myYear2',%constMyYear%);
unsigned4 myYear2 := 0 : stored('myYear2');

//Implement at runtime.
#STORED('myYear',(unsigned4)(stringlib.getDateYYYYMMDD()[1..4]));
unsigned4 myYear := 0 : stored('myYear');


output(myYear);
output(myYear2);
