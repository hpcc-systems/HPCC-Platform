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

IMPORT std;

unsigned numRecs := 10000 : STORED('numRecs');

rec1 := RECORD
 string20 key;
END;

seed := dataset([{'[A]\n'}], rec1);

rec1 addNodeNum(rec1 L, unsigned4 c) := TRANSFORM
 SELF.key := (string) c;
 SELF := L;
END;

one_per_node := DISTRIBUTE(NORMALIZE(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)),(unsigned)key);

rec1 generatePseudoRandom(rec1 L, unsigned4 c) := TRANSFORM
 SELF.key := '['+(string17) RANDOM()+']\n';
END;

bigstream := NORMALIZE(one_per_node, numRecs*(Std.System.Thorlib.node()+1), generatePseudoRandom(LEFT, COUNTER));

deskew := DISTRIBUTE(bigstream, SKEW(0.1));

SEQUENTIAL(
  IF (COUNT(bigstream) = COUNT(NOFOLD(deskew)),
     OUTPUT('Count after de-skew matched'),
     FAIL('ERROR: Count after de-skew did not match!')
  )
);
