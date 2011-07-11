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

//Illustates bug #19786 - l.y is potentially ambiguous in the context of the project if only done on type.
//And yes I have seen silly code like this!

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

xNamesRecord := record
dataset(namesRecord) matches;
        end;

namesTable := dataset('x',namesRecord,FLAT);

xnamesRecord t(namesRecord l) := transform
     dids := dataset(l);
     n := normalize(dids, 10, transform(left));
     p := project(n, transform(namesRecord, self.forename := l.forename, self := left));
     self.matches := p;
     end;

p2 := project(namesTable, t(left));

output(p2);
