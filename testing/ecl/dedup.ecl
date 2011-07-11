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

Val1Sort := SORT(SomeFile,Value1);
Val2Sort := SORT(SomeFile,Value2);

Dedup1   := DEDUP(Val1Sort,LEFT.Value1 = RIGHT.Value1);

/* Result set is:
    Rec#    Value1  Value2
    1       A       X
    2       B       G
    3       C       G
*/
Dedup2 := DEDUP(Val2Sort,LEFT.Value2 = RIGHT.Value2);

/* Result set is:
    Rec#    Value1  Value2
    1       A       B
    2       C       C
    3       B       G
    4       A       X
*/

Dedup3 := DEDUP(Val1Sort,LEFT.Value1 = RIGHT.Value1,RIGHT);

/* Result set is:
    Rec#    Value1  Value2
    1       A       B
    2       B       G
    3       C       C
*/

Dedup4 := DEDUP(Val2Sort,LEFT.Value2 = RIGHT.Value2,RIGHT);

/* Result set is:
    Rec#    Value1  Value2
    1       A       B
    2       C       C
    3       C       G
    4       A       X
*/

MyRec2 := RECORD
    UNSIGNED1 Value1;
    STRING1 Value2;
END;

SomeFile2 := dataset([{0, 'A'},{0, 'A'},{1, 'A'},{1, 'A'},{2, 'A'},{2, 'A'}], MyRec2);
LocalHashDedup := DEDUP(DISTRIBUTE(SomeFile2, Value1), Value2, HASH, LOCAL); // depending on # nodes, expect <= 3 results
MyRec2 PrFunc(MyRec2 rec, UNSIGNED c) := TRANSFORM
 SELF.Value1 := c;
 SELF := rec;
END;
P1 := PROJECT(LocalHashDedup, PrFunc(LEFT, COUNTER), LOCAL);
Dedup5 := DEDUP(P1, Value1=1); // should result in 1 record out

/* Result set is:
    Rec#    Value1  Value2
    1       1       A
*/

output(Dedup1);
output(Dedup2);
output(Dedup3);
output(Dedup4);
output(Dedup5);
