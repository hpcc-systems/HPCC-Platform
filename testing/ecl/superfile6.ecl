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
// Super File added to itself test
//noRoxie

string fsuper := '~t6::superfile';
string fsub := '~t6::subfile';
ds := DATASET ([{'aaa'}, {'bbb'}, {'ccc'}, {'ddd'}], {string name});

sequential (
  FileServices.DeleteSuperFile (fsuper),
  FileServices.DeleteLogicalFile (fsub),
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
