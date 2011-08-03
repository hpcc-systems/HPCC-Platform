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

#OPTION('thorKeys', 1);

ppersonRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

ppersonRecord2 := RECORD
ppersonRecord;
unsigned8 __filepos {virtual(fileposition)};
  END;

tmptable2 := dataset([
        {'801', 'YYY','Git', 13, 10},
        {'811', 'Smith','Horatio', 13, 10},
        {'901', 'Nicholson','Jack', 13, 10},
        {'902', 'Jackson','Samuel', 13, 10},
        {'903', 'Hardy','Laurel', 13, 10},
        {'904', 'Ford','Harrison', 13, 10},
        {'905', 'Jackson','Peter', 13, 10},
        {'906', 'Caine','Michael', 13, 10},
        {'907', 'Foster','Jodie', 13, 10},
        {'908', 'Smith','Joseph', 13, 10},
        {'908', 'Gillin','Philip', 13, 10},
        {'999', 'Laurel','Stan', 13, 10}], ppersonRecord);

pperson1 := DATASET('test.d00', { ppersonRecord,  unsigned8 __filepos {virtual(fileposition)}}, THOR);

i := INDEX(pperson1, { surname, __filepos }, 'test.idx');

d := DISTRIBUTE(tmptable2, i, left.surname = right.surname);

sss := sort(d, surname, LOCAL);

output(sss, , 'new.d00', OVERWRITE);

ssst := DATASET('new.d00', ppersonRecord2, THOR);

_sorted := SORTED(ssst, surname, __filepos);

tr := { ppersonRecord2.surname, ppersonrecord2.__filepos; };

tr t(_sorted l) := transform self:=l; end;

z := project(_sorted, t(left));

BUILDINDEX(z, , 'test2.idx', OVERWRITE);
