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

rs :=
RECORD
    string a;
    integer iField;
END;
ds := DATASET([{'a', 500}, {'a', 10}, {'g', 5}, {'q', 16}, {'e', 2}, {'d', 12}, {'c', 1}, {'f', 17}, {'a', 500}, {'a', 10}, {'g', 5}, {'q', 16}, {'e', 2}, {'d', 12}, {'c', 1}, {'f', 17}], rs);

#uniquename(fld1)
#uniquename(fld2, 'Field$')
#uniquename(fld3, 'Field')
#uniquename(fld4, 'Total__$__')

output(nofold(ds), { string %fld1% := a; integer %fld2% := iField; integer %fld3% := iField*2; integer %fld4% := iField*3; });
