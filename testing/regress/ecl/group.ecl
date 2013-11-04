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

SortedRecs  := SORT(SomeFile,Value1);

GroupedRecs1 := GROUP(SomeFile,Value1);     // unsorted
GroupedRecs2 := GROUP(SortedRecs,Value1);   // sorted

SortedRecs1 := SORT(GroupedRecs1,Value2);
SortedRecs2 := SORT(GroupedRecs1,-Value2);
SortedRecs3 := SORT(GroupedRecs2,Value2);
SortedRecs4 := SORT(GroupedRecs2,-Value2);

OUTPUT(SortedRecs1);
OUTPUT(SortedRecs2);
OUTPUT(SortedRecs3);
OUTPUT(SortedRecs4);

/*
SortedRecs1 results in:
    Rec#    Value1  Value2
    1       C       C
    2       C       G
    3       A       X
    4       B       G
    5       A       B

SortedRecs2 results in:
    Rec#    Value1  Value2
    1       C       G
    2       C       C
    3       A       X
    4       B       G
    5       A       B

SortedRecs3 results in:
    Rec#    Value1  Value2
    1       A       B
    2       A       X
    3       B       G
    4       C       C
    5       C       G

SortedRecs4 results in:
    Rec#    Value1  Value2
    1       A       X
    2       A       B
    3       B       G
    4       C       G
    5       C       C
*/