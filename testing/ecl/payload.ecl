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

//UseStandardFiles
//Sample query for pulling across some sample related payload indexes

i1 := index({ string40 forename, string40 surname }, { unsigned4 id }, sqPersonIndexName);
i2 := index({ unsigned4 id }, { unsigned8 filepos }, sqPersonIndexName+'ID');

ds1 := i1(KEYED(forename = 'Liz'));
ds2 := JOIN(ds1, i2, KEYED(LEFT.id = RIGHT.id));
ds3 := FETCH(sqPersonExDs, ds2, RIGHT.filepos);

output(ds1);
output(ds2);
output(ds3);

