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

import std.system.thorlib;
//class=stress
unsigned numrecs := 2000000+0 : stored('numrecs');

string makeString(unsigned max) := BEGINC++
void * temp = rtlMalloc(max);
memset(temp, 92, max);
__lenResult = max;
__result = (char *)temp;
ENDC++;

rec := record, maxlength(2000000)
     unsigned4 len;
     unsigned4 node;
     string payload;
         string10 key;
       end;

seed := dataset([{0, 0, 'A', '0'}], rec);

rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF.node := c;
    SELF := L;
  END;

one_per_node := distribute(normalize(seed, thorlib.nodes(), addNodeNum(LEFT, COUNTER)), (unsigned) key);

rec generatePseudoRandom(rec L, unsigned4 c) := transform
    SELF.payload := MakeString(L.node*10);
    SELF.key := (string) RANDOM() + (string) RANDOM();
    SELF.len := LENGTH(SELF.payload)+LENGTH(SELF.key)+4+4;
    SELF.node := L.node;
  END;

bigstream := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, counter));  // 2 Gb per node.

thornodes := MAX(seed, thorlib.nodes()) : global;  // Force it to calculate nodes() on thor not hthor

output (bigstream,,'varskewed',overwrite);