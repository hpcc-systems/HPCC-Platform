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

valuesRecord := 
            RECORD
real4           value;
            END;

//valuesTable := dataset([{1.2},{2.2},{3.3},{10.0}], valuesRecord);
valuesTable := dataset('x',valuesRecord,thor);
string5 xy(real4 a)     := (string5)if(a!=9.9,(string5)a,'     ');

real4 rnum_fin(real4 per_id) := (real4)xy(per_id);
//real4 rnum_fin(string5 per_id) := ((real4)per_id)+0.0;

integer rangex(real4 yy) := MAP(
(yy >= 0.0 AND yy < 1.0) => 1,
(yy >= 1.0 AND yy < 2.0) => 2,
(yy >= 2.0 AND yy < 3.0) => 3,
4);

output(valuesTable, {a := value; x := rnum_fin(valuesTable.value), rnum_fin(valuesTable.value) > 2.1, rangex(rnum_fin(valuesTable.value)) });
