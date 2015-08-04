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

// Works:

// le := {DATA val {MAXLENGTH(99999)}};
// DATA99999 arraye  := x'00';
// dse := DATASET([{arraye}], le);

// datadse := DATASET('~temp::dataarrayein', le, THOR);
// DATA99999 arraye1  := datadse[1].val;

// dse1 := DATASET([{arraye1}], le);


// SEQUENTIAL(output(dse, , '~temp::dataarrayein', OVERWRITE)
// , output(dse1, , '~temp::dataarraye1', OVERWRITE)
// );

// Throws a segfault:

le := {DATA val {MAXLENGTH(100000)}};
DATA100000 arraye  := x'00';
dse := DATASET([{arraye}], le);

datadse := DATASET('~temp::dataarrayein', le, THOR);
DATA100000 arraye1  := datadse[1].val;

dse1 := DATASET([{arraye1}], le);

SEQUENTIAL(
output(dse, , '~temp::dataarrayein', OVERWRITE)
, output(dse1, , '~temp::dataarraye1', OVERWRITE)
);