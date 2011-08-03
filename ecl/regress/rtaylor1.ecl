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
    string20  name;
    unsigned4 sequence;
    unsigned4 value;
  END;

seed100 := dataset([{'',1,0},{'',2,0},{'',3,0},{'',4,0},{'',5,0},{'',6,0},{'',7,0},{'',8,0},{'',9,0},
                    {'',1,1},{'',2,1},{'',3,1},{'',4,1},{'',5,1},{'',6,1},{'',7,1},{'',8,1},{'',9,1},
                    {'',1,2},{'',2,2},{'',3,2},{'',4,2},{'',5,2},{'',6,2},{'',7,2},{'',8,2},{'',9,2},
                    {'',1,3},{'',2,3},{'',3,3},{'',4,3},{'',5,3},{'',6,3},{'',7,3},{'',8,3},{'',9,3},
                    {'',1,4},{'',2,4},{'',3,4},{'',4,4},{'',5,4},{'',6,4},{'',7,4},{'',8,4},{'',9,4}
                   ], rec);

sortedseed100 :=  sort(seed100 ,sequence,value);

rec makeRec(rec L, rec R, string name) := TRANSFORM
    self.name := name;
    self.sequence := L.sequence;
    self.value := R.value;
END;

output(ROLLUP(group(sortedseed100, sequence),
       left.sequence=right.sequence,
       makeRec(left, right, 'grouped')));

