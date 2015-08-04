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

idRecord := { unsigned id; };

namesRecord :=
            RECORD
string        surname;
string        forename;
dataset(idRecord) ids;
            END;

namesTable := dataset([
    {'Fred', 'Smith', [1,2,3]},
    {'Fred', 'Smith', [1,2,4]},
    {'John', 'Smith', [4,5,6]},
    {'John', 'Smith', [4,5,6]}], namesRecord);

output(dedup(NOFOLD(namesTable), ALL));
