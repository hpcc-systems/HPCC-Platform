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
