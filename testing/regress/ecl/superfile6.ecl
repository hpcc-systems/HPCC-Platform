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

import Std.File AS FileServices;
// Super File added to itself test

#option('slaveDaliClient', true);

string fsuper := '~t6::superfile';
string fsub := '~t6::subfile';
ds := DATASET ([{'aaa'}, {'bbb'}, {'ccc'}, {'ddd'}], {string name});

conditionalDelete(string lfn) := FUNCTION
        RETURN IF(FileServices.FileExists(lfn), FileServices.DeleteLogicalFile(lfn));
END;

sequential (
  FileServices.DeleteSuperFile (fsuper),
  conditionalDelete (fsub),
  output(FileServices.SuperFileExists (fsuper)),
  output(FileServices.FileExists (fsub)),

  // Creates a super with a sub and check
  OUTPUT (ds, , fsub, OVERWRITE),
  FileServices.CreateSuperfile (fsuper),
  FileServices.AddSuperFile (fsuper, fsub),
  output(FileServices.SuperFileExists (fsuper)),
  output(FileServices.FileExists (fsub)),

  // Tries to add the super to it, should fail
  FileServices.AddSuperFile (fsuper, fsuper),

  // Should still be there
  output(FileServices.SuperFileExists (fsuper)),
  output(FileServices.FileExists (fsub)),
  output('done')
)
