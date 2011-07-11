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