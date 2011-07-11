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

namesRecord := 
            RECORD
string10        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'X','Z'}], namesRecord);

outr :=     RECORD
string10        a := 'Gavin'[1..4];
string10        b := namesTable.surname[1..4];
string10        c := '!'+'Gavin'[1..4]+'!';
string10        d := '!'+namesTable.surname[1..4]+'!';
string10        e := 'Gavin'[1..6];
string10        f := '!'+'Gavin'[1..6]+'!';
string10        g := trim(namesTable.surname)[4..]+'$';
string10        h := namesTable.surname[namesTable.age DIV 10..];
            END;

output(nofold(namesTable),outr,'out.d00',overwrite);
