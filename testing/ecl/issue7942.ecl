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

outrec := record
    integer id;
    string20 desc;
end;

aData := DATASET([{1234}],{ integer id});
bData := DATASET([{1234,'BigRed'}],outrec);

outrec xform(aData l, bData r) := transform
    self.id := l.id;
    self.desc := if (r.desc <> '', r.desc, '');
end;

zero := 0 : stored('zero');
one := 1 : stored('one');

abData1 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(0));
abData2 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(2));
abData3 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(one));
abData4 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(zero));

sequential(
output(abData1);                                                         
output(abData2);
output(abData3);
output(abData4);
);                                                         
