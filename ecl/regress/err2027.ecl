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

aaa := DATASET('aaa', {STRING1 f1, BOOLEAN f2}, thor);

sorted_aaa := SORT(aaa,f1);
joined_aaa := SORT(aaa, f1, JOINED(sorted_aaa));

//sorted_aaa1 := SORT(aaa,f1,f2);
//OUTPUT(SORT(aaa, f1, JOINED(joined_aaa)));

OUTPUT(SORT(aaa, f1, JOINED(SORT(aaa, f1, JOINED(sorted_aaa)));
