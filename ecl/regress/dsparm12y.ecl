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

// test: more than one level of dataset passing (w/ mapping)
// 

ds := dataset('ds', {String10 first_name; string20 last_name; }, FLAT);

integer f(virtual dataset({String10 first_name}) d) := count(d(first_name = 'fred'));

dataset g(virtual dataset({String20 last_name}) d) := d(last_name='tom');      

integer h(virtual dataset d) := count(g(d))+f(d);

ct := h(ds);

ct;
