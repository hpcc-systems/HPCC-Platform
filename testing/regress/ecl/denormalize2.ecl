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

//#########################################
//Set Execution Flags 

doTestSimple       := true;     //automated = true
doTestGrouped      := true;     //automated = true
doTestFiltered     := true;     //automated = true


//#########################################
//Prepare Test Data 
MyRec := RECORD
  STRING1 Value1;
  integer1 Value2;
END;
ParentFile := DATASET([{'H',10},
                       {'G',10},
                       {'F',10},
                       {'E',10},
                       {'D',10},
                       {'C',20},
                  {'B',20},
                       {'A',20}],MyRec);
SortedParent := SORT(ParentFile,Value2);
//GroupedParent := GROUP(SortedParent,Value1);

ChildFile := sort(DATASET([{'H',1},
                      {'G',1},
                      {'F',1},
                      {'E',1},
                      {'D',1},
                      {'C',1},
                      {'B',1},
                      {'C',2},
                      {'C',3},
                      {'C',4},
                      {'B',2},
                      {'B',3},
                      {'A',1},
                      {'A',2}],MyRec),value1,value2);

OutRec := RECORD
  string20  name := '';
  integer1  ChildCount := 0;
  ParentFile.Value1;
  ParentFile.Value2;
  integer1 ChildVal1 := 99;
  integer1 ChildVal2 := 99;
  integer1 ChildVal3 := 99;
  integer1 ChildVal4 := 99;
END;

P_Recs := TABLE(ParentFile, OutRec);
GP_Recs := GROUP(TABLE(SortedParent, OutRec),Value2);

//#########################################
//All TRANSFORMs 

outrec makeRec(outrec L, MyRec R, string name) := TRANSFORM
    self.name := name;
    self.ChildCount := L.ChildCount + 1;
    self.ChildVal1 := IF(R.Value2 = 1, R.Value2, L.ChildVal1);
    self.ChildVal2 := IF(R.Value2 = 2, R.Value2, L.ChildVal2);
    self.ChildVal3 := IF(R.Value2 = 3, R.Value2, L.ChildVal3);
    self.ChildVal4 := IF(R.Value2 = 4, R.Value2, L.ChildVal4);
    self := L;
END;

outrec makeRecCtr(outrec L, MyRec R, integer C, string name) := TRANSFORM
    self.name := name;
    self.ChildCount := C;
    self.ChildVal1 := IF(R.Value2 = 1, R.Value2, L.ChildVal1);
    self.ChildVal2 := IF(R.Value2 = 2, R.Value2, L.ChildVal2);
    self.ChildVal3 := IF(R.Value2 = 3, R.Value2, L.ChildVal3);
    self.ChildVal4 := IF(R.Value2 = 4, R.Value2, L.ChildVal4);
    self := L;
END;

outrec makeRecSkip(outrec L, MyRec R, string name) := TRANSFORM
    self.name := name;
    self.ChildCount := IF((INTEGER1)R.Value2 = 2, SKIP, L.ChildCount + 1);
    self.ChildVal1 := IF(R.Value2 = 1, R.Value2, L.ChildVal1);
    self.ChildVal2 := IF(R.Value2 = 2, R.Value2, L.ChildVal2);
    self.ChildVal3 := IF(R.Value2 = 3, R.Value2, L.ChildVal3);
    self.ChildVal4 := IF(R.Value2 = 4, R.Value2, L.ChildVal4);
    self := L;
END;

outrec makeRecCtrSkip(outrec L, MyRec R, integer c, string name) := TRANSFORM
    self.name := name;
    self.ChildCount := IF(C = 2, SKIP, C);
    self.ChildVal1 := IF(C = 1, 1, L.ChildVal1);
    self.ChildVal2 := IF(C = 2, 2, L.ChildVal2);
    self.ChildVal3 := IF(C = 3, 3, L.ChildVal3);
    self.ChildVal4 := IF(C = 4, 4, L.ChildVal4);
    self := L;
END;

//#########################################
//All MACROs 

// Simple denormalize, no unkeyed filter....
MAC_simpledenorm(result, leftinput, rightinput, name, filters='TRUE') := MACRO
result :=   PARALLEL(output(dataset([{'Simple Denormalize: ' + name}], {string80 _Process})),
                     output(SORT(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRec(left, right, 'simple')),value1)),
                     output(count(choosen(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRec(left, right, 'simple choosen')),1))),
                     output(SORT(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecSkip(left, right, 'skip, simple')),value1)),
                     output(count(choosen(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecSkip(left, right, 'skip, simple choosen')),1))),
                     output(dataset([{'Counter Denormalize: ' + name}], {string80 _Process})),
                     output(SORT(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecCtr(left, right, counter, 'counter')),value1)),
                     output(count(choosen(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecCtr(left, right, counter, 'counter choosen')),1))),
                     output(SORT(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecCtrSkip(left, right, counter, 'skip, counter')),value1)),
                     output(count(choosen(DENORMALIZE(leftinput(filters), rightinput(filters), left.value1=right.value1, makeRecCtrSkip(left, right, counter, 'skip, counter choosen')),1)))
)
ENDMACRO;


//#########################################
//All MACRO Calls 

MAC_simpledenorm(straightdenorm,P_recs,Childfile,'straight');
MAC_simpledenorm(straightdenormgrouped,GP_recs,Childfile,'grouped');

MAC_simpledenorm(straightdenormfiltered,P_recs,Childfile,'filtered',value1 <> 'C');
MAC_simpledenorm(straightdenormgroupedfiltered,GP_recs,Childfile,'grouped filtered',value1 <> 'C');

//#########################################
//All Actions 
           #if (doTestSimple)
             straightdenorm;
           #end
           #if (doTestGrouped)
             straightdenormgrouped;
           #end
           #if (doTestFiltered)
             #if (doTestSimple)
               straightdenormfiltered;
             #end
             #if (doTestGrouped)
               straightdenormgroupedfiltered;
             #end
           #end
           output(dataset([{'Completed'}], {string20 _Process}));
