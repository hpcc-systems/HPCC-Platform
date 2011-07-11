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

//noRoxie
//nolocal


import lib_fileservices;

baserec := RECORD
    STRING6 name{xpath('thisisthename')};
    INTEGER8 blah{xpath('thisistheblah')};
    STRING9 value{xpath('thisisthevalue')};
END;

baseset := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], baserec);

o1 := output(baseset, , 'regress::base.d00', OVERWRITE, UPDATE);

b1 := DATASET('regress::base.d00', baserec, FLAT);

ded0 := DEDUP(b1, name);


j := JOIN(baseset+ded0, ded0, LEFT.blah = RIGHT.blah);

baserecfp := RECORD
 baserec;
 UNSIGNED8 filepos{virtual(fileposition)};
END;
ids := DATASET('regress::base.d00', baserecfp, FLAT);
i := INDEX(ids, {name, blah, filepos}, 'regress::outi1.idx');

o2_1 := output(choosen(ded0, 50) , , 'regress::out1.d00', OVERWRITE, UPDATE);
o2_2 := output(choosen(ded0, 100) , , 'regress::out2.d00', OVERWRITE, UPDATE);
o2_3 := output(choosen(ded0, 150) , , 'regress::out3.d00', OVERWRITE, UPDATE);
o2_4 := output(choosen(ded0, 200) , , 'regress::out4.d00', OVERWRITE, UPDATE);

o3_1 := output(ded0+ded0, , 'regress::out5.d00', OVERWRITE, UPDATE);

o4_1 := OUTPUT(j, , 'regress::out6.d00', OVERWRITE, UPDATE);
o4_2 := OUTPUT(j, , 'regress::out7.d00', OVERWRITE);

o5_1 := BUILDINDEX(i, OVERWRITE, UPDATE);

SEQUENTIAL(
o1,
PARALLEL(o2_1, o2_2, o2_3, o2_4, o3_1, o4_1, o5_1),

o1,
PARALLEL(o2_1, o2_2, o2_3, o2_4, o3_1),
PARALLEL(o4_1, OUTPUT('non-update branch')), o4_1,
o5_1,

FileServices.DeleteLogicalFile('regress::base.d00'),
FileServices.DeleteLogicalFile('regress::out1.d00'),
FileServices.DeleteLogicalFile('regress::out2.d00'),
FileServices.DeleteLogicalFile('regress::out3.d00'),
FileServices.DeleteLogicalFile('regress::out4.d00'),
FileServices.DeleteLogicalFile('regress::out5.d00'),
FileServices.DeleteLogicalFile('regress::out6.d00'),
FileServices.DeleteLogicalFile('regress::outi1.idx')
);
