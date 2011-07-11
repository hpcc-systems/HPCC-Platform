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