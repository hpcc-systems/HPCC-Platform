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

dobrec := record
  unsigned id;
  string8  dob1;
  string8  dob2;
  string8  dob3;
end;

singledob := record
  string8 dob;
end;

maindata := record
   unsigned id;
   dataset(singledob) mydobs;
end;

main := dataset('main', maindata, thor);
dobs := dataset('dobs', dobrec, thor);

maindata myjoin(main L, dobs R) := transform
   self.mydobs := dataset([{R.dob1},{R.dob2},{R.dob3}],singledob);
   self := l;
end;


output(join(main, dobs, left.id = right.id, myjoin(left, right)));

