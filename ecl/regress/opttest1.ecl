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

#option ('optimizeGraph', false);
rec := record
 string10 lname; 
 string10 fname;  
end;

head := dataset('infile',rec,flat);


x1 := table(head, {lname, fname, unsigned4 cnt := 1 });

x2 := sort(x1, lname);

x3 := table(x2, {fname, lname, cnt});

x4 := x3(lname > 'Halliday');

output(x4);

/*
i) swap filter x4 with table x3.
ii) swap filter x4' with sort x2
iii) swap filter x4'' with table x1
*/
