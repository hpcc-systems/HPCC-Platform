/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
