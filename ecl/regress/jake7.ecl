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

ppersonRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;
ppersonRecord2 := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

pperson1 := DATASET('in.d00', ppersonRecord, THOR);

tmptable := dataset([
        {'001', 'Halliday','Gavin', 13, 10},
        {'002', 'Smith','Jake', 13, 10},
        {'003', 'Hicks','Nigel', 13, 10},
        {'004', 'Gillin','Pete', 13, 10}], ppersonRecord);


ppersonRecord2 tr1 (ppersonRecord l)
:=
    transform
                   self := l;
    end;

ppersonRecord JoinTransform (ppersonRecord l, ppersonRecord2 r)
:=
    transform
                   self := l;
                   self := r;
    end;


p1 := project(tmptable, tr1(LEFT));


h1 := DISTRIBUTE(tmptable, HASH(surname));
s1 := sort(nofold(h1), surname);

g1 := group(s1, surname);

j1 := join(g1, p1,
        LEFT.id = RIGHT.id,
        JoinTransform (LEFT, RIGHT));

output(j1);

