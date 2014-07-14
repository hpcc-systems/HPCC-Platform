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

import Std.File;

rec := RECORD
    STRING6 name;
    INTEGER8 blah;
    STRING9 value;
END;

ds := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], rec);

SEQUENTIAL(
  OUTPUT(ds, , '~regress::renametest.d00', OVERWRITE),
  File.RenameLogicalFile('~regress::renametest.d00', '~regress::afterrename1.d00'),
  File.RenameLogicalFile('~regress::afterrename1.d00', '~regress::scope1::scope2::afterrename2.d00'),
  File.RenameLogicalFile('~regress::scope1::scope2::afterrename2.d00', '~regress::scope1::afterrename3.d00'),
  OUTPUT(DATASET('~regress::scope1::afterrename3.d00', rec, FLAT)),
  File.DeleteLogicalFile('~regress::scope1::afterrename3.d00')
);
