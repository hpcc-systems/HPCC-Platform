/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    This program is free software: you can redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#option ('importAllModules', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
            END;

entryRecord := RECORD
unsigned4       id;
namesRecord     name1;
namesRecord     name2;
            END;

inTable := dataset('x',entryRecord,THOR);

p := project(inTable, transform(namesRecord, SELF := IF(LEFT.id > 100, LEFT.name1, LEFT.name2)));

output(p);
