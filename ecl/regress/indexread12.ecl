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

__set_debug_option__('targetClusterType', 'hthor');

d := dataset('~local::rkc::person', 
{ string10 f1, 
  unsigned4 f2;
  unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\seisint\\person.name_first.key', sorted);

string5 s5 := '' : stored('s5');
string10 s10 := '' : stored('s10');
string15 s15 := '' : stored('s15');

unsigned2 u2 := 0 : stored('u2');
unsigned4 u4 := 0 : stored('u4');
unsigned8 u8 := 0 : stored('u8');

output(i(keyed(f1 = s5)));      // should be ok
output(i(keyed(f1 = s10)));     // should be ok exact match
output(i(keyed(f1 = s15)));     // should be ok, with a post filter



