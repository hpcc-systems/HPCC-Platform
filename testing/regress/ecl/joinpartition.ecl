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

IMPORT std.system.thorlib;

UNSIGNED numrecs := 2000000 : STORED('numrecs');

rec := RECORD
 UNSIGNED8 key;
END;

seed := DATASET([{0}], rec);

rec addNodeNum(rec L, UNSIGNED4 c) := transform
 SELF.key := c-1;
 SELF := L;
END;

one_per_node := DISTRIBUTE(NORMALIZE(seed, thorlib.nodes(), addNodeNum(LEFT, COUNTER)), (UNSIGNED4) key);

rec generatePseudoRandom(rec L, UNSIGNED4 c) := TRANSFORM
 SELF.key := (L.key * numrecs) + c;
END;

bigstream := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, COUNTER));

smlstream := SAMPLE(bigstream, 100);

joinPartitionL := JOIN(bigstream, smlstream, LEFT.key=RIGHT.key, PARTITION LEFT); // default
joinPartitionR := JOIN(smlstream, bigstream, LEFT.key=RIGHT.key, PARTITION RIGHT);

rec checksort(rec l, rec r) := TRANSFORM
 SELF.key := IF(l.key <= r.key, r.key, ERROR('ERROR - records not in sequence'));
 SELF := r;
END;

checksortedL := NOFOLD(ITERATE(joinPartitionL, checksort(LEFT, RIGHT), LOCAL));
checksortedR := NOFOLD(ITERATE(joinPartitionR, checksort(LEFT, RIGHT), LOCAL));
IF(COUNT(checksortedL) = COUNT(smlstream), OUTPUT('Join partition left succeeded'), FAIL('ERROR- count did not match expected'));
IF(COUNT(checksortedR) = COUNT(smlstream), OUTPUT('Join partition right succeeded'), FAIL('ERROR- count did not match expected'));
