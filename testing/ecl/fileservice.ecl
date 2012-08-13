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
