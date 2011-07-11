/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
