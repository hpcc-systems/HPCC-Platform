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
rec := record
    string20  name;
    unsigned4 value;
    unsigned4 sequence;
  END;

seed100 := dataset([{'',1,0},{'',2,0},{'',3,0},{'',4,0},{'',5,0},{'',6,0},{'',7,0},{'',8,0},{'',9,0},{'',10,0},
                    {'',11,0},{'',12,0},{'',13,0},{'',14,0},{'',15,0},{'',16,0},{'',17,0},{'',18,0},{'',19,0},{'',20,0},
                    {'',21,0},{'',22,0},{'',23,0},{'',24,0},{'',25,0},{'',26,0},{'',27,0},{'',28,0},{'',29,0},{'',30,0},
                    {'',31,0},{'',32,0},{'',33,0},{'',34,0},{'',35,0},{'',36,0},{'',37,0},{'',38,0},{'',39,0},{'',40,0},
                    {'',41,0},{'',42,0},{'',43,0},{'',44,0},{'',45,0},{'',46,0},{'',47,0},{'',48,0},{'',49,0},{'',50,0}
                   ], rec);

//#########################################
//All TRANSFORMs 
rec makeRec(rec L, rec R, string name) := TRANSFORM
    self.name := name;
    self.value := R.value;
    self.sequence := R.value + L.value;
END;

rec makeRecCtr(rec L, rec R, integer c, string name) := TRANSFORM
    self.name := name;
    self.value := R.value;
    self.sequence := c;
END;

rec makeRecSkip(rec L, rec R, string name) := TRANSFORM
    self.name := name;
    self.value := IF (R.value %2 = 1, SKIP, R.value);
    self.sequence := R.value + L.value;
END;

rec makeRecCtrSkip(rec L, rec R, integer c, string name) := TRANSFORM
    self.name := name;
    self.value := IF (R.value %2 = 1, SKIP, R.value);
    self.sequence := c;
END;

//#########################################
//All MACROs 

// Simple ITERATE, no unkeyed filter....
simpleITERATE(result, input, name, filters='TRUE') := MACRO
result :=   PARALLEL(output(dataset([{'','Simple ITERATE: ' + name}], {'',string80 _Process})),
                     output(ITERATE(input(filters), makeRec(left, right, 'simple'))),
                     output(choosen(ITERATE(input(filters), makeRec(left, right, 'simple choosen')),1)),
                     output(ITERATE(input(filters), makeRecSkip(left, right, 'skip, simple'))),
                     output(choosen(ITERATE(input(filters), makeRecSkip(left, right, 'skip, simple choosen')),1)),
                     output(dataset([{'','Counter ITERATE: ' + name}], {'',string80 _Process})),
                     output(ITERATE(input(filters), makeRecCtr(left, right, counter, 'counter'))),
                     output(choosen(ITERATE(input(filters), makeRecCtr(left, right, counter, 'counter choosen')),1)),
                     output(ITERATE(input(filters), makeRecCtrSkip(left, right, counter, 'skip, counter'))),
                     output(choosen(ITERATE(input(filters), makeRecCtrSkip(left, right, counter, 'skip, counter choosen')),1))
)
ENDMACRO;


//#########################################
//All MACRO Calls 

simpleITERATE(straightiter,seed100,'straight');
simpleITERATE(straightitergrouped,group(seed100, value),'grouped');

simpleITERATE(straightiterfiltered,seed100,'filtered',value > 3);
simpleITERATE(straightitergroupedfiltered,group(seed100, value),'grouped filtered',value > 3);

//#########################################
//All Actions 
           #if (doTestSimple)
             straightiter;
           #end
           #if (doTestGrouped)
             straightitergrouped;
           #end
           #if (doTestFiltered)
             #if (doTestSimple)
               straightiterfiltered;
             #end
             #if (doTestGrouped)
               straightitergroupedfiltered;
             #end
           #end
           output(dataset([{'','Completed'}], {'',string20 _Process}));
