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

// temporary hack to get around codegen optimizing platform(),once call into global (and therefore hthor) context.
nononcelib :=
    SERVICE
varstring platform() : library='graph', include='eclhelper.hpp', ctxmethod, entrypoint='getPlatform';
    END;

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
  IF (COUNT(rollup_ds) = IF(nononcelib.platform()='roxie',1,CLUSTERSIZE),
     output('Sort order verified'), 
     FAIL('ERROR: rollup count did not match expected!')
  ),
  OUTPUT('done')
);





