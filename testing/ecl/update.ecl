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

//noRoxie
//nolocal


import Std.File AS FileServices;

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
i := INDEX(ids, {name, blah, value, filepos}, 'regress::outi1.idx');

o2_1 := output(choosen(ded0, 50) , , 'regress::out1.d00', OVERWRITE, UPDATE);
o2_2 := output(choosen(ded0, 100) , , 'regress::out2.d00', OVERWRITE, UPDATE);
o2_3 := output(choosen(ded0, 150) , , 'regress::out3.d00', OVERWRITE, UPDATE);
o2_4 := output(choosen(ded0, 200) , , 'regress::out4.d00', OVERWRITE, UPDATE);

o3_1 := output(ded0+ded0, , 'regress::out5.d00', OVERWRITE, UPDATE);

o4_1 := OUTPUT(j, , 'regress::out6.d00', OVERWRITE, UPDATE);
o4_2 := OUTPUT(j, , 'regress::out7.d00', OVERWRITE);

o5_1 := BUILDINDEX(i, OVERWRITE, UPDATE);


conditionalDelete(string lfn) := FUNCTION
        RETURN IF(FileServices.FileExists(lfn), FileServices.DeleteLogicalFile(lfn));
END;

SEQUENTIAL(
conditionalDelete('regress::base.d00'),
conditionalDelete('regress::out1.d00'),
conditionalDelete('regress::out2.d00'),
conditionalDelete('regress::out3.d00'),
conditionalDelete('regress::out4.d00'),
conditionalDelete('regress::out5.d00'),
conditionalDelete('regress::out6.d00'),
conditionalDelete('regress::outi1.idx'),

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
