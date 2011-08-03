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


datafilename := '~jaketest::d2.d00';
idxfilename := '~jaketest::d2.idx';

baserec := RECORD
    STRING6 name;
    INTEGER6 blah;
    STRING9 value;
END;

_baseset := DATASET([{'fruit', 123, 'apple'}, {'car', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'book', 987, 'cch22'}], baserec);

baseset := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], baserec);

sort0 := SORT(baseset, name);
genbase := OUTPUT(sort0, , datafilename, OVERWRITE);

fpbaserec := RECORD
    baserec;
    UNSIGNED8 filepos{virtual(fileposition)};
END;

fpbaseset := DATASET(datafilename, fpbaserec, FLAT);

indexRec := RECORD
fpbaseset.name;
fpbaseset.filepos;
END;
idx := INDEX(fpbaseset, indexRec, idxfilename);

b1 := BUILDINDEX(idx, OVERWRITE);


// SEQUENTIAL(genbase, b1, OUTPUT(COUNT(idx)));
x := global(COUNT(idx));

baserec ff(baserec L) := TRANSFORM
    SELF.blah := x;
    SELF := L;
 END;
nx := PROJECT(baseset, ff(LEFT));

// OUTPUT(nx);
// SEQUENTIAL(genbase, b1);
SEQUENTIAL(genbase, b1, OUTPUT(nx));
// SEQUENTIAL(OUTPUT(nx));
