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
    unsigned4 value;
    unsigned4 sequence;
  END;

seed100 := dataset([{1,0},{2,0},{3,0},{4,0},{5,0},{6,0},{7,0},{8,0},{9,0},{10,0},
                    {11,0},{12,0},{13,0},{14,0},{15,0},{16,0},{17,0},{18,0},{19,0},{20,0},
                    {21,0},{22,0},{23,0},{24,0},{25,0},{26,0},{27,0},{28,0},{29,0},{30,0},
                    {31,0},{32,0},{33,0},{34,0},{35,0},{36,0},{37,0},{38,0},{39,0},{40,0},
                    {41,0},{42,0},{43,0},{44,0},{45,0},{46,0},{47,0},{48,0},{49,0},{50,0}
                   ], rec);

//#########################################
//All TRANSFORMs 
outrec := record
    string20  name;
    unsigned4 value;
    unsigned4  sequence;
END;

outrec makeRec(rec L, string name) := TRANSFORM
    self.name := name;
    self.value := L.value;
    self.sequence := L.value;
END;

outrec makeRecCtr(rec L, integer c, string name) := TRANSFORM
    self.name := name;
    self.value := L.value;
    self.sequence := c;
END;

outrec makeRecSkip(rec L, string name) := TRANSFORM
    self.name := name;
    self.value := IF (L.value %2 = 1, SKIP, L.value);
    self.sequence := L.value;
END;

outrec makeRecCtrSkip(rec L, integer c, string name) := TRANSFORM
    self.name := name;
    self.value := IF (L.value %2 = 1, SKIP, L.value);
    self.sequence := c;
END;

//#########################################
//All MACROs 

// Simple project, no unkeyed filter....
simpleproject(result, input, name, filters='TRUE') := MACRO
result :=   PARALLEL(output(dataset([{'Simple Project: ' + name}], {string80 _Process})),
                     output(PROJECT(input(filters), makeRec(left, 'simple'))),
                     output(choosen(PROJECT(input(filters), makeRec(left, 'simple choosen')),1)),
                     output(PROJECT(input(filters), makeRecSkip(left, 'skip, simple'))),
                     output(choosen(PROJECT(input(filters), makeRecSkip(left, 'skip, simple choosen')),1)),
                     output(dataset([{'Counter Project: ' + name}], {string80 _Process})),
                     output(PROJECT(input(filters), makeRecCtr(left, counter, 'counter'))),
                     output(choosen(PROJECT(input(filters), makeRecCtr(left, counter, 'counter choosen')),1)),
                     output(PROJECT(input(filters), makeRecCtrSkip(left, counter, 'skip, counter'))),
                     output(choosen(PROJECT(input(filters), makeRecCtrSkip(left, counter, 'skip, counter choosen')),1))
)
ENDMACRO;


//#########################################
//All MACRO Calls 

simpleproject(straightproj,seed100,'straight');
simpleproject(straightprojgrouped,group(seed100, value),'grouped');

simpleproject(straightprojfiltered,seed100,'filtered',value > 3);
simpleproject(straightprojgroupedfiltered,group(seed100, value),'grouped filtered',value > 3);

//#########################################
//All Actions 
           #if (doTestSimple)
             straightproj;
           #end
           #if (doTestGrouped)
             straightprojgrouped;
           #end
           #if (doTestFiltered)
             #if (doTestSimple)
               straightprojfiltered;
             #end
             #if (doTestGrouped)
               straightprojgroupedfiltered;
             #end
           #end
           output(dataset([{'Completed'}], {string20 _Process}));
