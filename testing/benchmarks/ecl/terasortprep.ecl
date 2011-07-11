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

IMPORT Std.System.thorlib;

// Generate standard terasort datafile (minutesort format)

unsigned8 totalrecs := 1000*1000*1000*1000/100;
unsigned8 numrecs := totalrecs/CLUSTERSIZE : stored('numrecs');                       // rows per node except last
unsigned8 numrecslast := totalrecs-(CLUSTERSIZE-1)*numrecs : stored('numrecslast');   // rows on last node

rec := record
     string10  key;
     string10  seq;
     string80  fill;
       end;

seed := dataset([{'0', '0', '0'}], rec);

rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.seq := (string) (c-1);
    SELF := L;
  END;

one_per_node := distribute(normalize(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)), (unsigned) seq);

rec fillRow(rec L, unsigned4 c) := transform

    SELF.key := (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32);

    unsigned4 n := ((unsigned4)L.seq)*numrecs+c;
    SELF.seq := (string10)n;
    unsigned4 cc := (n-1)*8;
    string1 c1 := (>string1<)((cc)%26+65);
    string1 c2 := (>string1<)((cc+1)%26+65);
    string1 c3 := (>string1<)((cc+2)%26+65);
    string1 c4 := (>string1<)((cc+3)%26+65);
    string1 c5 := (>string1<)((cc+4)%26+65);
    string1 c6 := (>string1<)((cc+5)%26+65);
    string1 c7 := (>string1<)((cc+6)%26+65);
    string1 c8 := (>string1<)((cc+7)%26+65);
    SELF.fill := c1+c1+c1+c1+c1+c1+c1+c1+c1+c1+
             c2+c2+c2+c2+c2+c2+c2+c2+c2+c2+
             c3+c3+c3+c3+c3+c3+c3+c3+c3+c3+
             c4+c4+c4+c4+c4+c4+c4+c4+c4+c4+
             c5+c5+c5+c5+c5+c5+c5+c5+c5+c5+
             c6+c6+c6+c6+c6+c6+c6+c6+c6+c6+
             c7+c7+c7+c7+c7+c7+c7+c7+c7+c7+
             c8+c8+c8+c8+c8+c8+c8+c8+c8+c8;
  END;

outdata := NORMALIZE(one_per_node, IF(thorlib.node()=thorlib.nodes()-1,numrecslast,numrecs), fillRow(LEFT, counter));  

OUTPUT(outdata,,'benchmark::terasort1',overwrite);
