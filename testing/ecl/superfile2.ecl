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

import Std.File AS FileServices;
// Super File regression test 2 (RemoveSuperFile)
//noRoxie

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
