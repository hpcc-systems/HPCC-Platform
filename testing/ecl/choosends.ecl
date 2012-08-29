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

idRecord := RECORD
unsigned        id;
            END;

namesRecord :=
            RECORD
string20        surname;
UNSIGNED1       cnt;
dataset(idRecord)   ids{choosen(3)} := dataset([],idRecord);
            END;

namesTable := dataset([
        {'Hawthorn',1},
        {'Hawthorn',2},
        {'Smithe',3},
        {'X',4}], namesRecord);

makeIds(unsigned num) := DATASET(num, transform(idRecord, SELF.id := COUNTER));

p := PROJECT(namesTable, TRANSFORM(namesRecord, SELF.ids := makeIds(LEFT.cnt); SELF := LEFT));
output(p);
