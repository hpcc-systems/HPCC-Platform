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

#option ('globalFold', false);
//#########################################
//Set Execution Flags

doPrepareFiles     := true;     //automated = true
doTestSimple       := true;     //automated = true
doTestFiltered     := false;        //automated = true

NumberRecsToSeed   := 5;        //automated = 5

DataFileName1 := 'fetchtest::file500';
KeyFileName1  := 'fetchtest::valuekey';

//#########################################

//Prepare Test Data
rec := record
    string20  name;
    unsigned4 value;
    unsigned4 sequence;
  END;

rec makeSequence(rec L, integer n) := TRANSFORM
    self.name := '';
    self.value := n;
    self.sequence := n;
  END;

rec makeDuplicate(rec L, integer n) := TRANSFORM
    self.sequence := n;
    self := L;
  END;

seed    := dataset([{'',NumberRecsToSeed,0}], rec);
seed100 := normalize(seed, LEFT.value, makeSequence(LEFT, COUNTER));
seed500 := normalize(seed100, LEFT.value, makeDuplicate(LEFT,COUNTER));

prepareFiles := output(seed500,,DataFileName1, overwrite);

recplus     := { rec, unsigned8 __fpos{virtual(fileposition)}};
file500     := dataset(DataFileName1, recplus, FLAT);

valuekey    := INDEX(file500, {value, __fpos}, KeyFileName1);

prepareKeys := buildindex(valuekey, overwrite);

//#########################################
//All TRANSFORMs

recplus makeRec(recplus L, string name) := TRANSFORM
    self.name := name;
    self := L;
END;

recplus makeRecSkip(recplus L, string name) := TRANSFORM
    self.name := if(L.Value=4,SKIP,name);
    self := L;
END;

//#########################################
//All MACROs

// Simple fetch, no unkeyed filter....
MAC_simplefetch(result, baseinput, keyinput, name, filters='TRUE') := MACRO
result :=   PARALLEL(output(dataset([{'Simple Fetch: ' + name}], {string80 _Process})),
                     output(keyinput(filters)),
                     output(FETCH(baseinput, keyinput(filters), right.__fpos, makeRec(left, 'simple'))),
                     output(choosen(FETCH(baseinput, keyinput(filters), right.__fpos, makeRec(left, 'simple choosen')),1)),
                     output(FETCH(baseinput, keyinput(filters), right.__fpos, makeRecSkip(left, 'skip, simple'))),
                     output(choosen(FETCH(baseinput, keyinput(filters), right.__fpos, makeRecSkip(left, 'skip, simple choosen')),1))
)
ENDMACRO;


//#########################################
//All MACRO Calls

MAC_simplefetch(straightfetch,File500,valuekey,'straight');

MAC_simplefetch(straightfetchfiltered,File500,valuekey,'filtered',value <> 2);
//#########################################
//All Actions
           #if (doPrepareFiles)
             prepareFiles;
             prepareKeys;
           #end
           #if (doTestSimple)
             straightfetch;
           #end
           #if (doTestFiltered)
             #if (doTestSimple)
               straightfetchfiltered;
             #end
           #end
           output(dataset([{'Completed'}], {string20 _Process}));
