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



checkIn(inval, searchSet = '[]') := macro
    (inVal = 0 or inVal in searchSet)
    endmacro;

mylist1 := [1,2,3];
mylist2 := [1,2,100];

searchValue := 100;
checkIn(100, mylist1);
checkIn(100, mylist2);
checkIn(100);

checkIn(searchValue, mylist1);
checkIn(searchValue, mylist2);
checkIn(searchValue);
