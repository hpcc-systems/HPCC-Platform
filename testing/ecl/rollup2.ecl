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
doTestSimple := true; //automated = true
doTestGrouped := true; //automated = true
doTestFiltered := true; //automated = true
//#########################################
//Prepare Test Data 
rec := record
string20 name;
unsigned4 sequence;
unsigned4 value;
END;
seed100 := dataset([{'',1,0},{'',2,0},{'',3,0},{'',4,0},{'',5,0},{'',6,0},{'',7,0},{'',8,0},{'',9,0},
{'',1,1},{'',2,1},{'',3,1},{'',4,1},{'',5,1},{'',6,1},{'',7,1},{'',8,1},{'',9,1},
{'',1,2},{'',2,2},{'',3,2},{'',4,2},{'',5,2},{'',6,2},{'',7,2},{'',8,2},{'',9,2},
{'',1,3},{'',2,3},{'',3,3},{'',4,3},{'',5,3},{'',6,3},{'',7,3},{'',8,3},{'',9,3},
{'',1,4},{'',2,4},{'',3,4},{'',4,4},{'',5,4},{'',6,4},{'',7,4},{'',8,4},{'',9,4}
], rec);
sortedseed100 := sort(seed100 ,sequence,value);
//#########################################
//All TRANSFORMs 
rec makeRec(rec L, rec R, string name) := TRANSFORM
self.name := name;
self.sequence := L.sequence;
self.value := R.value;
END;
rec makeRecSkip(rec L, rec R, string name) := TRANSFORM
self.name := name;
self.sequence := L.sequence;
self.value := IF (R.value in [3,4], SKIP, R.value);
END;
//#########################################
//All MACROs 
// Simple ROLLUP, no unkeyed filter....
simpleROLLUP(result, input, name, filters='TRUE') := MACRO
result := PARALLEL(output(dataset([{'Simple ROLLUP: ' + name}], {string80 _Process})),
output(ROLLUP(input(filters), left.sequence=right.sequence, makeRec(left, right, 'simple'))),
output(choosen(ROLLUP(input(filters), left.sequence=right.sequence, makeRec(left, right, 'simple choosen')),1)),
output(ROLLUP(input(filters), left.sequence=right.sequence, makeRecSkip(left, right, 'skip, simple'))),
output(choosen(ROLLUP(input(filters), left.sequence=right.sequence, makeRecSkip(left, right, 'skip, simple choosen')),1))
)
ENDMACRO;
//#########################################
//All MACRO Calls 
simpleROLLUP(straightrollup,sortedseed100,'straight');
simpleROLLUP(straightrollupgrouped,group(sortedseed100, sequence),'grouped');
simpleROLLUP(straightrollupfiltered,sortedseed100,'filtered',value <= 3);
simpleROLLUP(straightrollupgroupedfiltered,group(sortedseed100, sequence),'grouped filtered',value <= 3);
//#########################################
//All Actions 
#if (doTestSimple)
straightrollup;
#end
#if (doTestGrouped)
straightrollupgrouped;
#end
#if (doTestFiltered)
#if (doTestSimple)
straightrollupfiltered;
#end
#if (doTestGrouped)
straightrollupgroupedfiltered;
#end
#end
output(dataset([{'','Completed'}], {'',string20 _Process}));
/**/
