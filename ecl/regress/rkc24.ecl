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

dummyDS := dataset('not::really', { unsigned val, unsigned8 _fpos { virtual(fileposition)}}, thor);

i1 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random');
i2 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random1');
i3 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random2');
i4 := index(dummyDS, {val, _fpos}, 'thor_data50::key::super');

count(i1(val > 100));
count(i2(val > 100));
count(i3(val > 100));
//count(i4(val > 100));
