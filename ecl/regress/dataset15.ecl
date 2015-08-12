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


namesRecord :=
            RECORD,maxlength(999)
string      surname;
string      forename;
            END;


rec := record, maxlength(1234)
unsigned4       id;
namesRecord     x;
        end;

namesTable := dataset('x',rec,FLAT);

rec t(namesTable l) := transform
    self.x.surname := trim(l.x.surname);
    self := l;
    end;

output(project(namesTable, t(LEFT)));

