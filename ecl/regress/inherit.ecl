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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
            END;

personRecord :=
            RECORD(namesRecord)
integer2        age := 25;
            END;

personRecordEx :=
            RECORD(personRecord), locale('Somewhere')
                record
unsigned8       filepos{virtual(fileposition)};
                end;
            END;

personTable := dataset('x',personRecord,FLAT);
personTableEx1 := dataset('x1',personRecordEx,FLAT);
personTableEx2 := dataset('x2',personRecordEx,FLAT);


namesRecord t1(personRecord l) := TRANSFORM
            SELF := l;
        END;


output(project(personTableEx1, t1(LEFT)),,'out1.d00',overwrite);

personRecord t2(namesRecord l) := TRANSFORM
            SELF := l;
            SELF := [];
        END;


output(project(personTableEx2, t2(LEFT)),,'out2.d00',overwrite);


output(project(personTable, t2(LEFT)),,'out3.d00',overwrite);
