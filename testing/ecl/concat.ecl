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

MyRec := RECORD
    STRING2 Value1;
    STRING1 Value2;
END;

File1 := DATASET([{'A1','A'},
                 {'A2','A'},
                 {'A3','B'},
                 {'A4','B'},
                 {'A5','C'}],MyRec);

File2 := DATASET([{'B1','A'},
                 {'B2','A'},
                 {'B3','B'},
                 {'B4','B'},
                 {'B5','C'}],MyRec);

File3 := DATASET([{'C1','A'},
                 {'C2','A'},
                 {'C3','B'},
                 {'C4','B'},
                 {'C5','C'}],MyRec);

g1 := GROUP(file1, value2);
g2 := GROUP(file2, value2);
g3 := GROUP(file3, value2);


// concats that need not preserve order
output(SORT(file1+file2, value1, value2));
output(SORT(file1+file2+file3, value1, value2));
output(SORT(file1+file1+file1+file2, value1, value2));

// concats that preserve order
output(file1&file2);
output(file1&file2&file3);
output(file1&file1&file1&file2);

// concats that preserve grouping

// some inputs grouped - does NOT preserve grouping
output(SORT(TABLE(file1+g2, { value2, c := count(GROUP)} ), value2, c));
output(SORT(TABLE(file1+g2+g3, { value2, c := count(GROUP)} ), value2, c));
output(SORT(TABLE(file1+g1+g1+g2, { value2, c := count(GROUP)} ), value2, c));

output(TABLE(file1&g2, { value2, c := count(GROUP)} ));
output(TABLE(file1&g2&g3, { value2, c := count(GROUP)} ));
output(TABLE(file1&g1&g1&g2, { value2, c := count(GROUP)} ));

// all inputs grouped - preserves grouping
output(SORT(TABLE(g1+g2, { value2, c := count(GROUP)} ), value2, c));
output(SORT(TABLE(g1+g2+g3, { value2, c := count(GROUP)} ), value2, c));
output(SORT(TABLE(g1+g1+g1+g2, { value2, c := count(GROUP)} ), value2, c));

output(TABLE(g1&g2, { value2, c := count(GROUP)} ));
output(TABLE(g1&g2&g3, { value2, c := count(GROUP)} ));
output(TABLE(g1&g1&g1&g2, { value2, c := count(GROUP)} ));

