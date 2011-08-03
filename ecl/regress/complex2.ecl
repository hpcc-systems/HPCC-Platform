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

#option ('foldAssign', false);
#option ('globalFold', false);
SMatch(string s1, string s2) :=  MAP(s1=s2 =>0, 1);

NSMatch(string s1, string s2) :=
  IF ( s1='' or s2='',0, SMatch(s1,s2));

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
integer2        age2 := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);


namesRecord t(namesRecord l) := TRANSFORM
    SELF.age := IF(l.age > 20, NSMatch(l.forename,l.forename)+NSMatch(l.forename,l.forename), 1);
    SELF.age2 := IF(l.age > 10, NSMatch(l.surname,l.forename)+NSMatch(l.surname,l.forename), NSMatch(l.surname,l.forename)) + NSMatch(l.forename,l.forename);
    SELF := l;
    END;

x := project(namesTable, t(LEFT));
output(x);

