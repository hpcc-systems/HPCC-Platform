/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//Ensure that we test the case where the last field in a record is a variable length row.
#option ('varFieldAccessorThreshold', 1);

childRec := RECORD
    UNSIGNED id;
    STRING name;
    unsigned value;
END;

r := RECORD
   unsigned id;
   STRING hint;
   childRec child;
END;


ds := DATASET([
            { 1, 'Gavin', { 3, 'no', 12 } },
            { 2, 'James', { 5, 'yes', 1 } },
            { 3, 'Jane', { 3, 'madness', 12 } }], r);

p := PROJECT(NOFOLD(ds), TRANSFORM(r, SELF.id := COUNTER, SELF := LEFT));

OUTPUT(NOFOLD(p));
