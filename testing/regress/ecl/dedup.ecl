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
