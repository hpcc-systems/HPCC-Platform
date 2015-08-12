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

gavin := service
    unsigned4 FoldString(const string src) : eclrtl,library='dab',entrypoint='rtlStringFilter';
end;

simpleRecord := RECORD
unsigned4   person_id;
data10      per_surname;
    END;


string mkstring(data x) := (string)x;

simpleDataset := DATASET('test',simpleRecord,FLAT);


output(simpleDataset, {gavin.FoldString((string)per_surname)}, 'out.d00');
output(simpleDataset, {gavin.FoldString(mkstring(per_surname))}, 'out.d00');
