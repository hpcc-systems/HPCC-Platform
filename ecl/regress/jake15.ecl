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
integer6    did := 0;
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

tmptable := dataset([
  {'001', 'Hawthorn','Gavin', 13, 10},
  {'002', 'Smith','Zack', 13, 10},
  {'003', 'Hewit','Nigel', 13, 10},
  {'004', 'Gillin','Paul', 13, 10},
  {'005', 'Smith','Horatio', 13, 10}], ppersonRecord);

s1 := sort(tmptable, surname);

ppersonRecord1 := RECORD
ppersonRecord;
unsigned8 __filepos{virtual(fileposition)};
  END;

pperson1 := DATASET('test.d00', ppersonRecord1, THOR);

indexRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned8 __filepos{virtual(fileposition)};
  END;

i := INDEX(pperson1, indexRecord, 'test.idx');

ppersonRecord copy(ppersonRecord1 l, indexRecord r) := TRANSFORM
SELF := l;
END;

f := FETCH(pperson1, i(surname='Smith'), RIGHT.__filepos, copy(LEFT, RIGHT));

output(f);
