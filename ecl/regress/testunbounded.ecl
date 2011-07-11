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

IMPORT * from lib_stringlib;

UNSIGNED4 FindUnbounded(STRING src, STRING terminator) :=
  StringLib.StringUnboundedUnsafeFind(src, terminator);

STRING stored_blank := '' : STORED('stored_blank');
STRING5 stored_Gavin := 'Gavin' : STORED('stored_Gavin');

EXPORT TestFindUnbounded := MODULE

  EXPORT TestRuntime := MODULE
    EXPORT Test1 := ASSERT(FindUnbounded(TRANSFER(stored_Gavin + ' ' + stored_Gavin, string10), 'Gavin') = 1);
    EXPORT Test2 := ASSERT(FindUnbounded(TRANSFER(stored_Gavin + ' ' + stored_Gavin, string10), ' Gavin') = 6);
  END;

END;

output(TestFindUnbounded);
