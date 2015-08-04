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

//noRoxie
#option ('optimizeIndexSource', true);

trec := RECORD
 unsigned1 key;
 string1  v;
 unsigned2 node := 0;
END;

tmp := DATASET([{1, 'a'}, {2, 'b'}, {3, 'c'}, {4, 'd'}, {5, 'e'}], trec);

trec addNodeNum(trec L, unsigned4 c) := transform
    SELF.node := c;
    SELF := L;
  END;

norm := NORMALIZE(tmp, thorlib.nodes(), addNodeNum(LEFT, counter));

disttmp := DISTRIBUTE(norm, node);

trec func(trec L, unsigned4 c) := TRANSFORM
    SELF := L;
  END;
norm2 := NORMALIZE(disttmp, 5000, func(LEFT, counter));

o1 := OUTPUT(norm2, , '~regress::key::test.d00', OVERWRITE);

rec := RECORD
 trec;
 unsigned8 filepos {virtual(fileposition)};
END;

d := DATASET('~regress::key::test.d00', rec, FLAT);

iRec := RECORD
 d.key;
 d.filepos;
END;

idx := INDEX(d, iRec, '~regress::key::test.idx');

thornodes := MAX(CHOOSEN(tmp, 1), thorlib.nodes()) : global;  // Force it to calculate nodes() on thor not hthor
text := 'running on ' + thornodes + ' nodes.' : global;

SEQUENTIAL(
//output(text),
o1,
BUILDINDEX(idx, OVERWRITE),
OUTPUT((unsigned)(COUNT(idx(key>3))/thornodes))
/*,

BUILDINDEX(idx, OVERWRITE, FEW),
OUTPUT((unsigned)(COUNT(idx(key<3))/thornodes)),
BUILDINDEX(idx, OVERWRITE),
OUTPUT((unsigned)(COUNT(idx(key=5))/thornodes)),
BUILDINDEX(idx, OVERWRITE, FEW),
OUTPUT((unsigned)(COUNT(idx(key<>2))/thornodes))
*/
);
