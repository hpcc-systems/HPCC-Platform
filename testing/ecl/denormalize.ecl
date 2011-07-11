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
