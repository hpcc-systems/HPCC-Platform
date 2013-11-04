/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

