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

xstring := type
    export string1 load( string1 x) := x; 
    export string1 store(string1 x) := x; 
end;

r := RECORD
  xstring extra;
END;

ds := DATASET('test',r,FLAT);

// this is ok
//count(ds);

// this causes mem leak
output(ds,,'out.d00');
                   
