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

import Std.File;
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

rec := RECORD
    STRING6 name;
    INTEGER8 blah;
    STRING9 value;
END;

ds := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], rec);
ds2 := DATASET( prefix + 'afterrename1.d00', rec, flat);

string compareDatasets(dataset(rec) ds1, dataset(rec) ds2) := FUNCTION
   boolean result := (0 = COUNT(JOIN(ds1, ds2, left.name=right.name, FULL ONLY)));
   RETURN if(result, 'Pass', 'Fail');
END;


SEQUENTIAL(
  OUTPUT(ds, , prefix + 'renametest.d00', OVERWRITE),
  File.RenameLogicalFile(prefix + 'renametest.d00', prefix + 'afterrename1.d00'),
  File.RenameLogicalFile(prefix + 'afterrename1.d00', prefix + 'scope1::scope2::afterrename2.d00'),
  File.RenameLogicalFile(prefix + 'scope1::scope2::afterrename2.d00', prefix + 'scope1::afterrename3.d00'),
  OUTPUT(DATASET(prefix + 'scope1::afterrename3.d00', rec, FLAT)),
  File.DeleteLogicalFile(prefix + 'scope1::afterrename3.d00'),
  OUTPUT(ds, , prefix + 'renametest.d00', OVERWRITE),
  OUTPUT(ds, , prefix + 'afterrename1.d00', OVERWRITE),
  // Rename with overwrite allowed
  File.RenameLogicalFile(prefix + 'renametest.d00', prefix + 'afterrename1.d00', true),
  output(compareDatasets(ds, ds2)),
  File.DeleteLogicalFile(prefix + 'afterrename1.d00'),
);
