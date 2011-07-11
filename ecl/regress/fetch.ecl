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

d := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);

i := index(d, { string10 zname, string15 f_name := name, unsigned8 fpos{virtual(fileposition)} } , '~local::key::person');

myFilter := i.f_name between 'GAVIN' AND 'GILBERT';

ii := choosen(i((f_name[1]!='!') AND (myFilter or f_name[1..2]='XX' or 'ZZZ' = f_name[..3])), 10);      //should be or but not processed yet.


x := fetch(d, ii, RIGHT.fpos);
output(x);
