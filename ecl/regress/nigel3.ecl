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


rec := record
   string1 K1 := '!';
   string1 K2 := '!';
   string3 N := '0';
   string1 EOL := x'0a';
end;

rec TR(rec l,rec r) := TRANSFORM
   SELF.K1 := r.K1;
   SELF.K2 := r.K2;
   SELF.N := (string3)((integer)l.N + (integer)r.N);
   SELF.EOL := r.EOL;
end;


DS1 := dataset('testddru', rec, flat);
DS2 := dataset('testddrj', rec, flat);
J := join (DS1, DS2, LEFT.K1 = RIGHT.K1, TR (LEFT, RIGHT));

output(J,,'testddrj.out');


