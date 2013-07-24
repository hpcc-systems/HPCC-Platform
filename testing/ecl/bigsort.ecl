/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
unsigned numrecs := 20000000 : stored('numrecs');

rec := record
         string90 payload;
         string10 key;
       end;

seed := dataset([{'A', '0'}], rec);

rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF := L;
  END;

one_per_node := distribute(normalize(seed, thorlib.nodes(), addNodeNum(LEFT, COUNTER)), (unsigned) key);

rec generatePseudoRandom(rec L, unsigned4 c) := transform
    SELF.payload := (string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM();
    SELF.key := (string) RANDOM() + (string) RANDOM();
  END;

bigstream := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, counter));  // 2 Gb per node.

thornodes := MAX(seed, thorlib.nodes()) : global;  // Force it to calculate nodes() on thor not hthor

sortedrecs := sort(bigstream, key);

rec checksort(rec l, rec r) := TRANSFORM
  self.key := if (l.key <= r.key, r.key, ERROR('ERROR - records not in sequence'));
  self := r;
END;

checksorted := NOFOLD(iterate(sortedrecs, checksort(LEFT, RIGHT), LOCAL));

if(count(checksorted) = (thornodes*numrecs), output('Sort succeeded'), FAIL('ERROR- count did not match expected'));
