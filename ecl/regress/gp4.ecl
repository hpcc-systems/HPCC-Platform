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

sname := type
  export integer loadlength(string9 s) := 9;
  export integer load(string9 s) := (integer)s;
  export string9 store(integer s) := (string9)s;
  end;

LayoutDirectTV := record
 sname   qid;
 string1 dummy
end;
testdata := dataset ('temp6', LayoutDirectTV, flat);

max(testdata,testdata.dummy);

