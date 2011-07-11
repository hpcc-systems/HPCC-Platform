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
