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


phoneNumberRec := record
unsigned        code;
string8         num;
        end;

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
phoneNumberRec  p1;
phoneNumberRec  p2;
phoneNumberRec  p3;
phoneNumberRec  p4;
            END;

namesTable := dataset('x',namesRecord,FLAT);

simpleStringrec := record
string32        text;
            end;


simpleStringRec t(namesRecord l) := transform
    self.text := transfer(l, string32);
    end;

output(project(namesTable, t(LEFT)));

simpleStringRec t2(namesRecord l) := transform
    self.text := transfer(l.p1, string32);
    end;

output(project(namesTable, t2(LEFT)));


output(project(namesTable, t(row(transform(namesRecord, SELF.p1 := LEFT.p2; SELF.p2 := LEFT.p1; SELF := LEFT)))));
