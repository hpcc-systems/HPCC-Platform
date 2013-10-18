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
    STRING1  Value1;
    STRING1  Value2;
    INTEGER1 Value3;
END;

SomeFile := DATASET([{'C','G',1},
                     {'C','C',2},
                     {'A','X',3},
                     {'B','G',4},
                     {'A','B',5}],MyRec);

SortedRecs1 := SORT(SomeFile,Value1,Value2);
SortedRecs2 := SORT(SomeFile,-Value1,Value2);
SortedRecs3 := SORT(SomeFile,Value1,-Value2);
SortedRecs4 := SORT(SomeFile,-Value1,-Value2);

SortedRecs1[1].Value3;                  //result = 5
SortedRecs2[1].Value3;                  //result = 2
SortedRecs3[1].Value3;                  //result = 3
SortedRecs4[1].Value3;                  //result = 1
EVALUATE(SortedRecs1[1],Value3 + 10);   //result = 15
EVALUATE(SortedRecs2[1],Value3 + 10);   //result = 12
EVALUATE(SortedRecs3[1],Value3 + 10);   //result = 13
EVALUATE(SortedRecs4[1],Value3 + 10);   //result = 11