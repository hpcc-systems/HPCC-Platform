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

tRec1 :=
    record
        String15 K := '!';
        String9 N := '0';
        String1 EOL := x'0a';
    end;

tRec1 createSeqNum(tRec1 L, tRec1 R) :=
  transform
    self.K := R.K;
    self.N := (string9)(((integer4)L.N)+1);
    self.EOL := L.EOL
  end;

DS1 := dataset ('test01', tRec1, flat);

IT := iterate(DS1, createSeqNum(left, right));

output (IT ,tRec1,'testit.out');


