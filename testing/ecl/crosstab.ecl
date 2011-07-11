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

MyRec := RECORD
    STRING1  Value1;
    STRING1  Value2;
    INTEGER1 Value3;
END;
SomeFile := DATASET([{'C','G',1},
                     {'C','C',2},
                     {'A','X',3},
                     {'B','G',4},
                     {'A','B',5}],MyRec);
MyOutRec := RECORD
    SomeFile.Value1;
    GrpCount := COUNT(GROUP);
    GrpSum   := SUM(GROUP,SomeFile.Value3);
END;

MyTable := TABLE(SomeFile,MyOutRec,Value1);

OUTPUT(SORT(MyTable,value1));
/* MyTable result set is:
    Rec#    Value1  GrpCount    GrpSum
    1       A       2           8
    2       B       1           4
    3       C       2           3
*/
