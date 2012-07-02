/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

rec := RECORD
   unsigned x;
   unsigned value;
END;

ds := DATASET([{1,1},{2,2},{3,3}], rec); dds := DISTRIBUTE(ds, x) : PERSIST('dds');

a := AGGREGATE(dds, rec,
        TRANSFORM(rec, SELF.value := IF(RIGHT.x<>0, LEFT.Value*RIGHT.Value, LEFT.Value), SELF := LEFT),
         TRANSFORM(rec, SELF.value := RIGHT1.Value*RIGHT2.Value, SELF := RIGHT2));

OUTPUT(a);
