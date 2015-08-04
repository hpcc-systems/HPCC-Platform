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

baserec := RECORD
     STRING6 name{xpath('thisisthename')};
     INTEGER8 blah{xpath('thisistheblah')};
     STRING9 value{xpath('thisisthevalue')};
END;

baseset := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], baserec);

o1 := output(baseset, , '~base.d00', OVERWRITE);

b1 := DATASET('~base.d00', baserec, FLAT);

sort0 := SORT(b1, name);

o2_1 := output(choosen(sort0, 50) , , '~out2_2.d00', OVERWRITE);

o2_2 := output(choosen(sort0, 100) , , '~out2_1.d00', OVERWRITE);

o2_3 := output(choosen(sort0, 150) , , '~out2_3.d00', OVERWRITE);

o2_4 := output(choosen(sort0, 200) , , '~out2_4.d00', OVERWRITE);

PARALLEL(o2_1, o2_2, o2_3, o2_4);
