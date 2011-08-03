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

// Generate standard terasort datafile

import lib_fileservices;

LOADXML('<defaultscope/>');

unsigned8 numrecs := 10000000 : stored('numrecs');   // rows per node

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



#declare(MYCLUSTERSIZE)
#declare(CLUSTERINC)
#declare(CLUSTERMAX)
#set(MYCLUSTERSIZE,25)
#set(CLUSTERINC,25)
#set(CLUSTERMAX,400)

#loop
  #if (%MYCLUSTERSIZE%>%CLUSTERMAX%)
    #break
  #end
  #uniquename(one_per_node)
  #uniquename(outdata)
  #uniquename(in)
  #uniquename(radix)
  #uniquename(dist)

  %one_per_node% := distribute(normalize(seed, %MYCLUSTERSIZE%, addNodeNum(LEFT, COUNTER)), (unsigned) seq);
  %outdata% := NORMALIZE(%one_per_node%, numrecs, fillRow(LEFT, counter));
  %in% := DATASET('nhtest::in_'+'%MYCLUSTERSIZE%',rec,FLAT);
  unsigned4 %radix% := ((9023+%MYCLUSTERSIZE%) DIV %MYCLUSTERSIZE%);
  %dist% := DISTRIBUTE(%in%,(((((unsigned4)(>unsigned1<)key[1])-32)*95+ (unsigned4)(>unsigned1<)key[2]-32) DIV %radix%));

  sequential(
    OUTPUT(%outdata%,,'nhtest::in_'+'%MYCLUSTERSIZE%',overwrite),
    OUTPUT(SORT(%dist%,key,local),,'nhtest::out_'+'%MYCLUSTERSIZE%',overwrite),
    FileServices.DeleteLogicalFile('nhtest::in_'+'%MYCLUSTERSIZE%'),
    FileServices.DeleteLogicalFile('nhtest::out_'+'%MYCLUSTERSIZE%'),
    OUTPUT('Done '+'%MYCLUSTERSIZE%')
  );

  #set(MYCLUSTERSIZE,%MYCLUSTERSIZE%+%CLUSTERINC%)
#end







