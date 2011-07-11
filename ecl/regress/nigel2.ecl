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
                String15 K;
                String10 F1;
        end;

tRec2 := 
        record
                String5  F2;
                String15 K;
        end;

tRec3 := 
        record
                String15 K;
                String10 F1;
                String5  F2;
        end;

DS1 := dataset ('test01.d00', tRec1, flat);
DS2 := dataset ('test02.d00', tRec2, flat);

tRec3 JT (tRec1 l, tRec2 r) :=
    transform
       self.K := l.K;
       self.F1 := l.F1;
       self.F2 := r.F2;
    end;

J := join (DS1, DS2, LEFT.K[1..1] = RIGHT.K[1..1], JT (LEFT, RIGHT));

output (choosen (J, 100),,'test01.out');
