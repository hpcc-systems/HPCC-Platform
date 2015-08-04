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
