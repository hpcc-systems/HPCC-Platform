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
    STRING1 Value1;
    STRING1 Value2;
END;

SomeFile := DATASET([{'C','G'},
                     {'C','C'},
                     {'A','X'},
                     {'B','G'},
                     {'A','B'}],MyRec);
MyOutRec := RECORD
    SomeFile.Value1;
    INTEGER2 Occurrences := 1;
END;

MyOutRec RollThem(MyOutRec L, MyOutRec R) := TRANSFORM
    SELF.Occurrences := L.Occurrences + 1;
    SELF := L;
END;

SomeTable := TABLE(SomeFile,MyOutRec);

SortedTable := SORT(SomeTable,Value1);

RolledUpRecs := ROLLUP(SortedTable,
                       LEFT.Value1 = RIGHT.Value1,
                       RollThem(LEFT,RIGHT));

OUTPUT(RolledUpRecs );
 
/*
Processes as:
    LEFT   vs.  RIGHT
    1 (A)       2 (A)       - match, run transform, lose #2
    1 (A)       3 (B)       - no match, output #1
    3 (B)       4 (C)       - no match, output #3 
    4 (C)       5 (C)       - match, run transform, lose #5, output #4

Result set is:
    Rec#    Value1  Occurrences
    1       A       2
    2       B       1
    3       C       2
*/