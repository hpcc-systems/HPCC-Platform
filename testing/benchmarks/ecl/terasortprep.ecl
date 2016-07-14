/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

IMPORT Std.System.thorlib;

#option('crcReadEnabled', false);
#option('crcWriteEnabled', false);

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
