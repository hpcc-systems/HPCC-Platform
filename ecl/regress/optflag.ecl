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

d := dataset('regress::no::such::file', {string10 f}, FLAT, OPT);
output(d);
count(d);
output(d(f='NOT'));
count(d(f='NOT'));

p := PRELOAD(dataset('regress::no::such::file::either', {string10 f}, FLAT, OPT));
output(p);
count(p);
output(p(f='NOT'));
count(p(f='NOT'));

p2 := PRELOAD(dataset('regress::no::such::file::again', {string10 f}, FLAT, OPT), 2);
output(p2);
count(p2);
output(p2(f='NOT'));
count(p2(f='NOT'));

i := INDEX(d,{f},{},'regress::nor::this', OPT);
output(i);
count(i);
output(i(f='NOT'));
count(i(f='NOT'));

j := JOIN(d, i, KEYED(LEFT.f = right.f));
output(j);

j1 := JOIN(d(f='NOT'), d, KEYED(LEFT.f = right.f), KEYED(i));
output(j1);

output(FETCH(d, i(f='not'), 0));

