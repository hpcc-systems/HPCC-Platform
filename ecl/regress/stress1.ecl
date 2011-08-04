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

//class=stress
#option ('resourceMaxDistribute', 30);
#option ('resourceUseMpForDistribute', false);
unsigned numrecs := 25000000 : stored('numrecs');

LOADXML('<Root/>');
#declare(NUMDISTS)
#set(NUMDISTS,10)

rec := record
     string86 payload;
         string10 key;
       end;


rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF := L;
  END;

rec generatePseudoRandom(rec L, unsigned4 c) := transform
    SELF.payload := (string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM();
    SELF.key := (string) RANDOM() + (string) RANDOM();
  END;

rec checkdist(rec l, rec r) := TRANSFORM
  self.key := if ((l.key!='')and(hash(l.key)%CLUSTERSIZE != hash(r.key)%CLUSTERSIZE), ERROR('ERROR - records not all same hash!'), r.key);
  self := r;
END;

seed := dataset([{'A', '0'}], rec);

norm := normalize(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER));

#declare(I)
#set(I,1)
#loop
  #if (%I%>%NUMDISTS%)
    #break
  #end

  #uniquename(one_per_node)
  %one_per_node% := distribute(norm, (unsigned) key);

  #uniquename(bigstream)
  %bigstream% := NORMALIZE(%one_per_node%, numrecs, generatePseudoRandom(LEFT, counter));  // 2.5 Gb per node.

  #uniquename(distrecs)
  %distrecs% := DISTRIBUTE(%bigstream%, HASH(key));

  #uniquename(checkdistributed)
  %checkdistributed% := iterate(%distrecs%, checkdist(LEFT, RIGHT),local);

  if(count(%checkdistributed%) = (CLUSTERSIZE*numrecs), output('Distribute succeeded'), FAIL('ERROR- count did not match expected'));
//  output(IF(count(%checkdistributed%) = (CLUSTERSIZE*numrecs), 'Distribute succeeded', 'ERROR- count did not match expected'));

  #set(I,%I%+1)
#end
