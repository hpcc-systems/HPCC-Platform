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

// Testing DISTRIBUTE,MERGE


unsigned numrecs := 1000000 : stored('numrecs');


rec1 := record
         string20 key;
       end;

seed := dataset([{ '[A]\n'}], rec1);

rec1 addNodeNum(rec1 L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF := L;
  END;

one_per_node := DISTRIBUTE(NORMALIZE(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)),(unsigned)key);

rec1 generatePseudoRandom(rec1 L, unsigned4 c) := transform
    SELF.key := '['+(string17) RANDOM()+']\n';
  END;

bigstream := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, counter));  

sds := SORT(bigstream,key,local);


hdsds := DISTRIBUTE(sds,HASH(key),MERGE(key));

rec1 checksort(rec1 l, rec1 r) := TRANSFORM
  self.key := IF (l.key <= r.key, r.key, 
                  ERROR('ERROR: records not in sequence! "' + l.key + '", "'+ r.key + '"'));
  self := r;
END;


rollup_ds := ROLLUP(hdsds,checksort(LEFT, RIGHT),TRUE,LOCAL);


SEQUENTIAL(
  IF (COUNT(rollup_ds) = CLUSTERSIZE, 
     output('Sort order verified'), 
     FAIL('ERROR: rollup count did not match expected!')
  ),
  OUTPUT('done')
);





