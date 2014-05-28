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
// Super File regression test 2 (RemoveSuperFile)

string fsuper := '~superfile3';
string fsub := '~superfile4';
string fsample := '~subfile6';
ds := DATASET ([{'aaa'}, {'bbb'}, {'ccc'}, {'ddd'}], {string name});

sequential (
  FileServices.DeleteSuperFile (fsuper),
  FileServices.DeleteSuperFile (fsub),
  OUTPUT (ds, , fsample, OVERWRITE),
  FileServices.CreateSuperfile (fsuper),
  FileServices.CreateSuperfile (fsub),
  FileServices.AddSuperFile (fsuper, fsub),
  FileServices.AddSuperFile (fsub, fsample),
  FileServices.RemoveSuperFile (fsuper, fsub, true, true),
  output(FileServices.SuperFileExists (fsub)),
  output(FileServices.FileExists (fsample)),
  OUTPUT (ds, , fsample),
  FileServices.CreateSuperfile (fsub),
  FileServices.AddSuperFile (fsuper, fsub),
  FileServices.AddSuperFile (fsub, fsample),
  FileServices.StartSuperFileTransaction();
  FileServices.RemoveSuperFile (fsuper, fsub, true, true),
  FileServices.FinishSuperFileTransaction(true), // rollback
  output(FileServices.SuperFileExists (fsub)),
  output(FileServices.FileExists (fsample)),
  FileServices.StartSuperFileTransaction();
  FileServices.RemoveSuperFile (fsuper, fsub, true, true),
  FileServices.FinishSuperFileTransaction(),
  output(FileServices.SuperFileExists (fsub)),
  output(FileServices.FileExists (fsample)),
  FileServices.DeleteSuperFile (fsuper),
  output('done')
)
