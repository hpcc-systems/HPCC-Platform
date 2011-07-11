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

import lib_FileServices;

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

o1 := output(namesTable1(age>10),,'~testing::out1');
o2 := output(namesTable1(age<=10),,'~testing::out2');

a1 := parallel(o1, o2);
a2 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile('~testing::super','~testing::out1'),
    FileServices.AddSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

a3 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.RemoveSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction(),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

a5 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.RemoveSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

super := dataset('~testing::super', namesRecord, thor);
per := super(age <> 0) : persist('per');

a4 := output(per);

unsigned action := 4;
case (action, 
        1=>a1,
        2=>a2,
        3=>a3,
        5=>a5,
        a4);
