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

SomeRecord := RECORD
    STRING1  Value1;
    STRING1  Value2;
    INTEGER1 Value3;
END;

SomeFile := DATASET([{'C','G',1},
                     {'C','C',2},
                     {'A','X',3},
                     {'B','G',4},
                     {'A','B',5}],SomeRecord);

output(MAP(EXISTS(SomeFile(Value1 = 'C')) => 1   // exists
          ,EXISTS(SomeFile(Value2 = 'G')) => 2   // exists
          ,3));

output(MAP(EXISTS(SomeFile(Value1 = 'Z')) => 1   // does not exist
          ,EXISTS(SomeFile(Value2 = 'G')) => 2   // exists
          ,3));

output(MAP(EXISTS(SomeFile(Value1 = 'Z')) => 1   // does not exist
          ,EXISTS(SomeFile(Value2 = 'Z')) => 2   // does not exist
          ,3));
