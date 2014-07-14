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
ParentFile := DATASET([{'C','A'},
                       {'B','B'},
                       {'A','C'}],MyRec);
ChildFile := DATASET([{'C','X'},
                      {'B','S'},
                      {'C','W'},
                      {'B','Y'},
                      {'A','Z'},
                      {'A','T'}],MyRec);
MyOutRec := RECORD
  ParentFile.Value1;
  ParentFile.Value2;
  STRING1 CVal2_1 := '';
  STRING1 CVal2_2 := '';
END;
P_Recs := TABLE(ParentFile, MyOutRec);
OUTPUT(P_Recs);
/* P_Recs result set is:
    Rec#    Value1  PVal2       CVal2_1     CVal2_2
    1       C       A           
    2       B       B           
    3       A       C               */
MyOutRec DeNormThem(MyOutRec L, MyRec R, INTEGER C) := TRANSFORM
    SELF.CVal2_1 := IF(C = 1, R.Value2, L.CVal2_1);
    SELF.CVal2_2 := IF(C = 2, R.Value2, L.CVal2_2);
    SELF := L;
END;
DeNormedRecs := DENORMALIZE(P_Recs, ChildFile,
                            LEFT.Value1 = RIGHT.Value1,
                            DeNormThem(LEFT,RIGHT,COUNTER));
OUTPUT(DeNormedRecs);
/* DeNormedRecs result set is:
    Rec#    Value1  PVal2   CVal2_1 CVal2_2
    1       A       C       Z       T   
    2       B       B       Y       S
    3       C       A       X       W
 */

MyOutRec DeNormThemAll(ParentFile L, dataset(MyRec) R) := TRANSFORM
    SELF.CVal2_1 := R[1].Value2;
    SELF.CVal2_2 := R[2].Value2;
    SELF := L;
END;
DeNormedAllRecs := DENORMALIZE(ParentFile, ChildFile,
                            LEFT.Value1 = RIGHT.Value1, group,
                            DeNormThemAll(LEFT,rows(RIGHT)));
OUTPUT(DeNormedAllRecs);
