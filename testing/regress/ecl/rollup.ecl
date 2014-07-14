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