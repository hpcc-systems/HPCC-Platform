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

FlatRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
    STRING1 CVal2_1;
    STRING1 CVal2_2;
END;

FlatFile := DATASET([{'C','A','X','W'},
                     {'B','B','S','Y'},
                     {'A','C','Z','T'}],FlatRec);

OutRec := RECORD
    FlatFile.Value1;
    FlatFile.Value2;
END;
P_Recs := TABLE(FlatFile, OutRec);

OUTPUT(P_Recs);
/*
P_Recs result set is:
    Rec#    Value1  Value2
    1       C       A
    2       B       B   
    3       A       C
*/

OutRec NormThem(FlatRec L, INTEGER C) := TRANSFORM
    SELF.Value2 := CHOOSE(C,L.CVal2_1, L.CVal2_2);
    SELF := L;
END;
ChildRecs := NORMALIZE(FlatFile,2,NormThem(LEFT,COUNTER));

OUTPUT(ChildRecs);
/*
ChildRecs result set is:
    Rec#    Value1  Value2
    1       C       X
    2       C       W
    3       B       S
    4       B       Y   
    5       A       Z
    6       A       T
*/