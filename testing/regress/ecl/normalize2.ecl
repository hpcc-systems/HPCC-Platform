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

doOutputParent     := true;     //automated = true
doTestSimple       := true;     //automated = true
doTestGrouped      := true;     //automated = true
doTestFiltered     := true;     //automated = true

//#########################################
//Prepare Test Data 
InRec := RECORD
  integer1  ChildCount;
  STRING1 Value1;
  STRING1 Value2;
  STRING1 ChildVal1;
  STRING1 ChildVal2;
  STRING1 ChildVal3;
  STRING1 ChildVal4;
END;
DenormFile := DATASET([ {2,'A','B','1','2','',''},
                        {3,'B','A','1','2','3',''},
                        {4,'C','C','1','2','3','4'},
                        {1,'D','B','1','','',''},
                        {1,'E','A','1','','',''},
                        {1,'F','C','1','','',''},
                        {1,'G','B','1','','',''},
                        {1,'H','A','1','','',''}],InRec);

OutRec := RECORD
  string20  name := '';
  DenormFile.Value1;
  DenormFile.Value2;
END;

VOutRec := RECORD
  string  name;
  DenormFile.Value1;
  DenormFile.Value2;
END;

P_Recs := TABLE(DenormFile, OutRec);

//#########################################
//All TRANSFORMs 

outrec makeRec(inrec L, integer C, string name) := TRANSFORM
    self.name := name;
    self.Value2 := choose(C,L.ChildVal1,L.ChildVal2,L.ChildVal3,L.ChildVal4);
    self := L;
END;

outrec makeRecSkip(inrec L, integer C, string name) := TRANSFORM
    self.name := name;
    self.Value2 := choose(C,L.ChildVal1,SKIP,L.ChildVal3,L.ChildVal4);
    self := L;
END;

voutrec makeVRec(inrec L, integer C, string name) := TRANSFORM
    self.name := name;
    self.Value2 := choose(C,L.ChildVal1,L.ChildVal2,L.ChildVal3,L.ChildVal4);
    self := L;
END;

voutrec makeVRecSkip(inrec L, integer C, string name) := TRANSFORM
    self.name := name;
    self.Value2 := choose(C,L.ChildVal1,SKIP,L.ChildVal3,L.ChildVal4);
    self := L;
END;

//#########################################
//All MACROs 

// Simple normalize, no unkeyed filter....
MAC_simplenorm(result, leftinput, name, filters='TRUE') := MACRO
result :=   PARALLEL(
                     output(dataset([{'Simple normalize: ' + name}], {string80 _Process})),
                     output(NORMALIZE(leftinput(filters), left.childcount, makeRec(left, counter, 'simple'))),
                     output(choosen(NORMALIZE(leftinput(filters), left.childcount, makeRec(left, counter, 'simple choosen')),1)),
                     output(NORMALIZE(leftinput(filters), left.childcount, makeRecSkip(left, counter, 'skip, simple'))),
                     output(choosen(NORMALIZE(leftinput(filters), left.childcount, makeRecSkip(left, counter, 'skip, simple choosen')),1)),

                     output(dataset([{'Variable normalize: ' + name}], {string80 _Process})),
                     output(NORMALIZE(leftinput(filters), left.childcount, makeVRec(left, counter, 'simple'))),
                     output(choosen(NORMALIZE(leftinput(filters), left.childcount, makeVRec(left, counter, 'simple choosen')),1)),
                     output(NORMALIZE(leftinput(filters), left.childcount, makeVRecSkip(left, counter, 'skip, simple'))),
                     output(choosen(NORMALIZE(leftinput(filters), left.childcount, makeVRecSkip(left, counter, 'skip, simple choosen')),1))
                    )
ENDMACRO;


//#########################################
//All MACRO Calls 

MAC_simplenorm(straightnorm,DenormFile,'straight');
MAC_simplenorm(straightnormgrouped,GROUP(DenormFile,Value2),'grouped');

MAC_simplenorm(straightnormfiltered,DenormFile,'filtered',value1 <> 'C');
MAC_simplenorm(straightnormgroupedfiltered,GROUP(DenormFile,Value2),'grouped filtered',value1 <> 'C');

//#########################################
//All Actions 
           #if (doOutputParent)
             output(dataset([{'Parent File'}], {string20 _Process}));
             output(P_recs);
           #end
           #if (doTestSimple)
             straightnorm;
           #end
           #if (doTestGrouped)
             straightnormgrouped;
           #end
           #if (doTestFiltered)
             #if (doTestSimple)
               straightnormfiltered;
             #end
             #if (doTestGrouped)
               straightnormgroupedfiltered;
             #end
           #end
           output(dataset([{'Completed'}], {string20 _Process}));
