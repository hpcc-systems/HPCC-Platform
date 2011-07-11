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

namesRecord := RECORD
string3     id;
string10    surname ;
string10    forename;
string2     nl;
  END;

natRecord := RECORD
string3     id;
string10    nationality ;
string2     nl;
  END;

nameAndNatRecord := RECORD
string3     id := 1;
string10    surname := '' ;
string10    forename := '';
string10    nationality := '' ;
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

names := DATASET('names.d00', namesRecord, FLAT);
nationalities := DATASET('nat.d00', natRecord, FLAT);

sortedNames := SORT(names, surname);
sortedNats := SORT(nationalities, nationality);

nameAndNatRecord JoinTransform (namesRecord l, natRecord r)
:=
    transform
                   self := l;
                   self := r;
    end;

JoinedSmEn := join (sortedNames, sortedNats,
        LEFT.id = RIGHT.id,
        JoinTransform (LEFT, RIGHT));

output(JoinedSmEn, , 'out.d00');
