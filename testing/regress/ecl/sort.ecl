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

MyVarRec := RECORD
    STRING1 Value1;
    IFBLOCK (SELF.Value1 < 'B')
      STRING1 Value2;
    END;
END;

SomeVarFile := DATASET([{'C'},
                     {'C'},
                     {'A','X'},
                     {'B'},
                     {'A','B'}],MyVarRec);

SortedRecs1 := SORT(SomeFile,Value1,Value2);
SortedRecs2 := SORT(SomeFile,-Value1,Value2);
SortedRecs3 := SORT(SomeFile,Value1,-Value2);
SortedRecs4 := SORT(SomeFile,-Value1,-Value2);
SortedRecs5 := SORT(SomeFile,Value2,Value1);
SortedRecs6 := SORT(SomeFile,-Value2,Value1);
SortedRecs7 := SORT(SomeFile,Value2,-Value1);
SortedRecs8 := SORT(SomeFile,-Value2,-Value1);

OUTPUT(SortedRecs1,{Value1,Value2});
OUTPUT(SortedRecs2,{Value1,Value2});
OUTPUT(SortedRecs3,{Value1,Value2});
OUTPUT(SortedRecs4,{Value1,Value2});
OUTPUT(SortedRecs5,{Value2,Value1});
OUTPUT(SortedRecs6,{Value2,Value1});
OUTPUT(SortedRecs7,{Value2,Value1});
OUTPUT(SortedRecs8,{Value2,Value1});

OUTPUT(choosen(SortedRecs1, 2),{Value1,Value2});
OUTPUT(choosen(SortedRecs2, 0),{Value1,Value2});

SortedVarRecs1 := SORT(SomeVarFile,Value1,Value2);
SortedVarRecs2 := SORT(SomeVarFile,-Value1,Value2);
SortedVarRecs3 := SORT(SomeVarFile,Value1,-Value2);
SortedVarRecs4 := SORT(SomeVarFile,-Value1,-Value2);
SortedVarRecs5 := SORT(SomeVarFile,Value2,Value1);
SortedVarRecs6 := SORT(SomeVarFile,-Value2,Value1);
SortedVarRecs7 := SORT(SomeVarFile,Value2,-Value1);
SortedVarRecs8 := SORT(SomeVarFile,-Value2,-Value1);

OUTPUT(SortedVarRecs1,{Value1,Value2});
OUTPUT(SortedVarRecs2,{Value1,Value2});
OUTPUT(SortedVarRecs3,{Value1,Value2});
OUTPUT(SortedVarRecs4,{Value1,Value2});
OUTPUT(SortedVarRecs5,{Value2,Value1});
OUTPUT(SortedVarRecs6,{Value2,Value1});
OUTPUT(SortedVarRecs7,{Value2,Value1});
OUTPUT(SortedVarRecs8,{Value2,Value1});

OUTPUT(choosen(SortedVarRecs1, 2),{Value1,Value2});
OUTPUT(choosen(SortedVarRecs2, 0),{Value1,Value2});
OUTPUT(choosen(SortedVarRecs3, 2, 2),{Value1,Value2});
OUTPUT(choosen(SortedVarRecs4, 0, 2),{Value1,Value2});

/*
SortedRecs1 results in:
    Rec#    Value1  Value2
    1       A       B
    2       A       X
    3       B       G
    4       C       C
    5       C       G

SortedRecs2 results in:
    Rec#    Value1  Value2
    1       C       C
    2       C       G
    3       B       G
    4       A       B
    5       A       X

SortedRecs3 results in:
    Rec#    Value1  Value2
    1       A       X
    2       A       B
    3       B       G
    4       C       G
    5       C       C

SortedRecs4 results in:
    Rec#    Value1  Value2
    1       C       G
    2       C       C
    3       B       G
    4       A       X
    5       A       B
*/
