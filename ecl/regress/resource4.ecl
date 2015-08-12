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

#option ('staticResource',1);
#option ('filteredReadSpillThreshold',0);
#option ('optimizeGraph', false);

baseRecord :=
            RECORD
unsigned8       id;
string20        surname;
string30        forename;
unsigned8       filepos{virtual(fileposition)}
            END;

baseTable := DATASET('base', baseRecord, THOR);

filteredTable := baseTable(surname <> 'Hawthorn');

//-------------------------------------------

x := IF(count(filteredTable) > 99, filteredTable(forename <> 'Gavin'), filteredTable(forename <> 'Mia'));

output(x,,'out.d00');
