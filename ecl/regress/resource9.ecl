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

__set_debug_option__('optimizeDiskRead',0);

baseRecord :=
            RECORD
unsigned8       id;
string20        surname;
string30        forename;
unsigned8       filepos{virtual(fileposition)}
            END;

baseTable1 := DATASET('base', baseRecord, THOR);

baseRecord t(baseRecord l) :=
    TRANSFORM
        SELF := l;
    END;

//-------------------------------------------

x1 := baseTable1(surname <> 'Hawthorn');

count(x1);
max(x1, surname);
min(x1, surname);

output(x1(forename <> 'x'),,'out.d00');

