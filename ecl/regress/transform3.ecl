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
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

namesIdRecord := 
            RECORD
namesRecord;
unsigned        id;
            END;

namesTable := dataset('x',namesRecord,FLAT);


projected := project(namesTable, transform(namesIdRecord, self := left; self := []));
output(projected);


//Not sure why you would want to do this... but we may as well be consistent.
namesIdRecord doClear := 
        transform
            self := [];
        end;

projected2 := project(namesTable, doClear);
output(projected2);


//functions of other transforms

namesIdRecord assignId(namesRecord l, unsigned value) :=
        TRANSFORM
            SELF.id := value;
            SELF := l;
        END;


assignId1(namesRecord l) := assignId(l, 1);
assignId2(namesRecord l) := assignId(l, 2);


//more check for mismatched types.

//more ad projects!


output(project(namesTable, assignId1(LEFT)));
output(project(namesTable, assignId2(LEFT)));

