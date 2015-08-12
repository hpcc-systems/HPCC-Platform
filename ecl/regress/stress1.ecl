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
