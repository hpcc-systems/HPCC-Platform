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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

// wrong def parm
attr := 1;
m0( t = 1) := MACRO 1 endmacro;
m1( t = attr) := MACRO 1 endmacro;
m2( t = 1+2) := macro 1 endmacro;
m3( t = person) := macro 1 endmacro;
m4( t = person.per_first_name) := macro 1 endmacro;

// check for error recovery
m0();
m1();
m2();
m3();
m4();

export myMacro(t1, t2 = '2 + 3') :=  macro
    t1+t2
  endmacro;

// too many parms
mymacro(1,2,3);

// no def value
mymacro();
mymacro(,);
mymacro(,2);

