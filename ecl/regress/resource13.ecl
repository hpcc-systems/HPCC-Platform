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

#option ('optimizeGraph', false);
__set_debug_option__('optimizeDiskRead', 0);

baseRecord :=
            RECORD
unsigned8       id;
string20        surname;
string30        forename;
unsigned8       age;
            END;

//baseTable := DATASET('base', baseRecord, THOR);

integer xxthreshold := 10           : stored('threshold');

baseTable := nofold(dataset([
        {1, 'Hawthorn','Gavin',31},
        {2, 'Hawthorn','Mia',30},
        {3, 'Smithe','David',10},
        {4, 'Smithe','Pru',10},
        {5, 'Stott','Mia',30},
        {6, 'X','Z', 99}], baseRecord));


filteredTable := baseTable(surname <> 'Hawthorn');

//-------------------------------------------

x := filteredTable(id < count(filteredTable));

output(x,,'out.d00',overwrite);
