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

#option ('countIndex', true); // workaround to bug #13110

import std.system.thorlib;

//noRoxie

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

o1 := OUTPUT(norm2, , 'regress::key::test.d00', OVERWRITE);

rec := RECORD
 trec;
 unsigned8 filepos {virtual(fileposition)};
END;

d := DATASET('regress::key::test.d00', rec, FLAT);

iRec := RECORD
 d.key;
 d.filepos;
END;

// NB: avoid reusing same key file (see bug #13112)
idx := INDEX(d, iRec, '~regress::key::test.idx');
idx2 := INDEX(d, iRec, '~regress::key::test2.idx');
idx3 := INDEX(d, iRec, '~regress::key::test3.idx');
idx4 := INDEX(d, iRec, '~regress::key::test4.idx');

thornodes := MAX(CHOOSEN(tmp, 1), thorlib.nodes()) : global;  // Force it to calculate nodes() on thor not hthor

SEQUENTIAL(
o1,
BUILDINDEX(idx, OVERWRITE), 
OUTPUT((unsigned)(COUNT(idx(key>3))/thornodes)),
BUILDINDEX(idx2, OVERWRITE, FEW),
OUTPUT((unsigned)(COUNT(idx2(key<3))/thornodes)),
BUILDINDEX(idx3, OVERWRITE),
OUTPUT((unsigned)(COUNT(idx3(key=5))/thornodes)),
BUILDINDEX(idx4, OVERWRITE, FEW),
OUTPUT((unsigned)(COUNT(idx4(key<>2))/thornodes))
);
