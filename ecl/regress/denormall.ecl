/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#option ('targetClusterType', 'roxie');

MyRec := RECORD
  STRING1 Value1;
  STRING1 Value2;
END;
ChildRec := RECORD(myRec)
  unsigned value3;
END;
ParentFile := DATASET([{'D','A'},
                       {'C','A'},
                       {'B','B'},
                       {'A','C'}],MyRec);
ChildFile := DATASET([{'C','X', 1},
                      {'B','S', 2},
                      {'C','W', 4},
                      {'B','Y', 8},
                      {'E','?', 16},
                      {'A','Z', 32},
                      {'A','T', 64}],ChildRec);
ChildIndex := index(ChildFile, { value1, value2 }, { value3 }, 'i');
//buildindex(ChildIndex,overwrite);

MyOutRec := RECORD
  ParentFile.Value1;
  ParentFile.Value2;
  STRING1 CVal2_1 := '';
  STRING1 CVal2_2 := '';
  unsigned sum3 := 0;
  unsigned cnt := 0;
END;
P_Recs := TABLE(ParentFile, MyOutRec);
/* P_Recs result set is:
    Rec#    Value1  PVal2       CVal2_1     CVal2_2
    1       C       A
    2       B       B
    3       A       C               */

MyOutRec DeNormThem(MyOutRec L, ChildRec R, INTEGER C) := TRANSFORM
    SELF.CVal2_1 := IF(C = 1, R.Value2, L.CVal2_1);
    SELF.CVal2_2 := IF(C = 2, R.Value2, L.CVal2_2);
    SELF.cnt := c;
    SELF.sum3 := L.sum3 + R.value3;
    SELF := L;
END;

outputDenormalize(flags) := macro
output(sort(DENORMALIZE(P_Recs, ChildFile, LEFT.Value1 = RIGHT.Value1, DeNormThem(LEFT,RIGHT,COUNTER) #expand(flags)), value1))
endmacro;

doStandardDenormalize(flags) := macro
output('Standard denormalize: ' + flags);
outputDenormalize(flags);
//output(sort(DENORMALIZE(P_Recs, ChildFile, LEFT.Value1 <=> RIGHT.Value1 = 0, DeNormThem(LEFT,RIGHT,COUNTER),all #expand(flags)), value1));
outputDenormalize(flags+',hash');
outputDenormalize(flags+',many lookup');
output(sort(DENORMALIZE(P_Recs, ChildIndex, LEFT.Value1 = RIGHT.Value1, DeNormThem(LEFT,row(right, ChildRec),counter) #expand(flags)), value1));
endmacro;

doStandardDenormalize(',inner');
doStandardDenormalize(',left outer');
doStandardDenormalize(',left only');

outputDenormalize(',right outer');
outputDenormalize(',right only');
outputDenormalize(',full outer');
outputDenormalize(',full only');

MyOutRec DeNormThem2(ParentFile L, dataset(ChildRec) R) := TRANSFORM
    SELF.CVal2_1 := R[1].Value2;
    SELF.CVal2_2 := R[2].Value2;
    self.cnt := count(r);
    SELF.sum3 := sum(R, value3);
    SELF := L;
END;

MyOutRec DeNormThem3(ParentFile L, dataset(recordof(ChildIndex)) R) := TRANSFORM
    SELF.CVal2_1 := R[1].Value2;
    SELF.CVal2_2 := R[2].Value2;
    self.cnt := count(r);
    SELF.sum3 := sum(R, value3);
    SELF := L;
END;

outputGroupDenormalize(flags) := macro
output(sort(DENORMALIZE(ParentFile, ChildFile, LEFT.Value1 = RIGHT.Value1, group, DeNormThem2(LEFT,rows(RIGHT)) #expand(flags)),value1));
endmacro;

doGroupDenormalize(flags) := macro
output('Group denormalize: ' + flags);
outputGroupDenormalize(flags);
//output(sort(DENORMALIZE(ParentFile, ChildFile, LEFT.Value1 <=> RIGHT.Value1 = 0, group, DeNormThem2(LEFT,rows(RIGHT)), all #expand(flags)),value1));
outputGroupDenormalize(flags+',hash');
outputGroupDenormalize(flags+',many lookup');
output(sort(DENORMALIZE(ParentFile, ChildIndex, LEFT.Value1 = RIGHT.Value1, group, DeNormThem2(LEFT,project(rows(right), ChildRec)) #expand(flags)), value1));
output(sort(DENORMALIZE(ParentFile, ChildIndex, LEFT.Value1 = RIGHT.Value1, group, DeNormThem3(left, rows(right)) #expand(flags)), value1));
endmacro;

doGroupDenormalize('');
doGroupDenormalize(',left outer');
doGroupDenormalize(',left only');

outputGroupDenormalize(',right outer');
outputGroupDenormalize(',right only');
outputGroupDenormalize(',full outer');
outputGroupDenormalize(',full only');
