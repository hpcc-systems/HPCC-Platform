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

dsfile := 'aaa.d00';
idxfile := 'aaa.idx';

myRecord := record
unsigned2       extra;
            end;

tt := DATASET([{1, 2}, {1, 256}], {UNSIGNED2 id, myRecord extra});

op := OUTPUT(tt, , dsfile, OVERWRITE);

ds := DATASET(dsfile, {tt, UNSIGNED8 fp{virtual(fileposition)}}, FLAT);

idx := INDEX(ds, {id}, {extra, fp}, idxfile);

bld := BUILD(idx, OVERWRITE);

mk := SEQUENTIAL(op, bld);

mk;


output(idx);
