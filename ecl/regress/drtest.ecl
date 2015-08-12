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

fixedRecord :=
        RECORD
string20            forename;
string20            surname;
string2             nl := '\r\n';
        END;

variableRecord :=
        RECORD
string              forename;
string              surname;
string2             nl := '\r\n';
        END;

fixedRecord var2Fixed(variableRecord l) :=
    TRANSFORM
        SELF := l;
    END;

variableRecord fixed2Var(fixedRecord l) :=
    TRANSFORM
        SELF.forename := TRIM(l.forename);
        SELF.surname := TRIM(l.surname);
        SELF := l;
    END;

/*
    Disk reading,
    i) project the dataset - which will can then be combined using the optimizeDiskRead
    ii) two null filters, to force a splitter.
    iii) outputs that can then be compared.

    Should test
    i) with/without optimize disk read
    ii) with/without crc checking.

*/

fixedDs := DATASET('dtfixed', fixedRecord, THOR);
variableDs := DATASET('dtvar', variableRecord, THOR);

output(SORT(variableDS, surname,forename),,'dtvar0');
newVariable := GROUP(SORT(PROJECT(fixedDs, fixed2Var(LEFT)),forename),forename);
//newVariable := GROUP(PROJECT(fixedDs, fixed2Var(LEFT)),forename);
newVar1 := newVariable(forename <> 'ZZZZZZZZZZ');
newVar1b := SORT(GROUP(newVar1),surname,forename);
output(newVar1b,,'dtvar1');
newVar2 := newVariable(surname <> 'ZZZZZZZZZZ');
newVar2b := SORT(GROUP(newVar2),surname,forename);
output(newVar2b,,'dtvar2');
newVar3 := SORT(GROUP(newVariable),surname,forename);
output(newVar3,,'dtvar3');

output(SORT(fixedDS, surname,forename),,'dtfixed0');
newFixed := GROUP(SORT(PROJECT(variableDs, var2Fixed(LEFT)),forename),forename);
//newFixed := GROUP(PROJECT(variableDs, var2Fixed(LEFT)),forename);
newFixed1 := newFixed(forename <> 'ZZZZZZZZZZ');
newFixed1b := SORT(GROUP(newFixed1),surname,forename);
output(newFixed1b,,'dtfixed1');
newFixed2 := newFixed(surname <> 'ZZZZZZZZZZ');
newFixed2b := SORT(GROUP(newFixed2),surname,forename);
output(newFixed2b,,'dtfixed2');
newFixed3 := SORT(GROUP(newFixed),surname,forename);
output(newFixed3,,'dtfixed3');
