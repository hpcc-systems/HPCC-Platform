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
