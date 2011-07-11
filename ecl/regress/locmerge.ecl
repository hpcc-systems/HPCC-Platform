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

unsigned numrecs := 1000 : stored('numrecs');

trec := record
         unsigned4 seq;
         unsigned4 strm;
         unsigned4 nodenum;
         unsigned4 key;
       end;

seed1 := dataset([{0, 1, 0, 0}], trec);
seed2 := dataset([{0, 2, 0, 0}], trec);
seed3 := dataset([{0, 3, 0, 0}], trec);
seed4 := dataset([{0, 4, 0, 0}], trec);
seed5 := dataset([{0, 5, 0, 0}], trec);

trec addNodeNum(trec L, unsigned4 c) := transform
    SELF.nodenum := c-1;
    SELF := L;
  END;

dataset one_per_node(dataset(trec) seed) := distribute(normalize(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)), nodenum);

trec generatePseudoRandom(trec L, unsigned4 c) := transform
    SELF.seq := c;   
    SELF.key := (RANDOM()%900);
    SELF := L;
  END;

dataset bigstream(dataset(trec) seed) := NORMALIZE(one_per_node(seed), numrecs, generatePseudoRandom(LEFT, counter)); 
dataset sortedrecs(dataset(trec) seed) := sort(bigstream(seed), key, local); 
mergedrecs :=      merge(sortedrecs(seed1),sortedrecs(seed2),sortedrecs(seed3),sortedrecs(seed4),sortedrecs(seed5), local);

trec checksort(trec l, trec r) := TRANSFORM
    SELF.key := if (l.key <= r.key, 
                r.key, 
                ERROR('ERROR - records not in order!'));
    SELF.seq := if ((l.key != r.key) or (l.strm <= r.strm),
                r.seq, 
                    ERROR('ERROR - records not in sequence!'));
    SELF := L;
END;


checksorted := iterate(distributed(mergedrecs,nodenum), checksort(LEFT, RIGHT), local);

if(count(checksorted) = CLUSTERSIZE*5*numrecs, output('Merge succeeded'), FAIL('ERROR (1) - count did not match expected'));

output(mergedrecs);