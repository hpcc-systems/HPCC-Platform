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

//#option ('optimizeProjects', false)
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

unsignedRec := { unsigned value };

rec2 := record
    dataset(unsignedRec) last3Values;
  END;

seed100 := dataset([{'',1,0},{'',2,0},{'',3,0},{'',4,0},{'',5,0},{'',6,0},{'',7,0},{'',8,0},{'',9,0},{'',10,0},
                    {'',11,0},{'',12,0},{'',13,0},{'',14,0},{'',15,0},{'',16,0},{'',17,0},{'',18,0},{'',19,0},{'',20,0},
                    {'',21,0},{'',22,0},{'',23,0},{'',24,0},{'',25,0},{'',26,0},{'',27,0},{'',28,0},{'',29,0},{'',30,0},
                    {'',31,0},{'',32,0},{'',33,0},{'',34,0},{'',35,0},{'',36,0},{'',37,0},{'',38,0},{'',39,0},{'',40,0},
                    {'',41,0},{'',42,0},{'',43,0},{'',44,0},{'',45,0},{'',46,0},{'',47,0},{'',48,0},{'',49,0},{'',50,0}
                   ], rec);

//#########################################
//All TRANSFORMs 

handler(rec L, rec2 R, string name) := module

    shared nextValue := IF(count(r.last3Values) < 3, 0, r.last3Values[1].value);

    export rec makeRec() := TRANSFORM
        self.name := name;
        self.value := l.value;
        self.sequence := nextValue;
    END;

    export rec makeRecCtr(integer c) := TRANSFORM
        self.name := name;
        self.value := l.value;
        self.sequence := c;
    END;

    export rec makeRecSkip() := TRANSFORM
        self.name := name;
        self.value := IF (l.value %2 = 1, SKIP, l.value);
        self.sequence := nextValue;
    END;

    export rec makeRecCtrSkip(integer c) := TRANSFORM
        self.name := name;
        self.value := IF (l.value %2 = 1, SKIP, l.value);
        self.sequence := c;
    END;

    export rec2 makeRec2() := transform
        self.last3Values := IF(count(r.last3Values) < 3, r.last3Values, r.last3Values[2..3]) + row(transform(unsignedRec, self.value := l.value));
    end;
END;

//#########################################
//All MACROs 

initialRow := row(transform(rec2, self := []));

// Simple PROCESS, no unkeyed filter....
simplePROCESS(result, input, name, filters='TRUE') := MACRO
result :=   PARALLEL(output(dataset([{'','Simple PROCESS: ' + name}], {'',string80 _Process})),
                     output(PROCESS(input(filters), initialRow, handler(left, right, 'simple').makeRec(), handler(left, right, 'simple').makeRec2())),
                     output(choosen(PROCESS(input(filters), initialRow, handler(left, right, 'simple choosen').makeRec(), handler(left, right, 'simple').makeRec2()),1)),
                     output(PROCESS(input(filters), initialRow, handler(left, right, 'skip, simple').makeRecSkip(), handler(left, right, 'simple').makeRec2())),
                     output(choosen(PROCESS(input(filters), initialRow, handler(left, right, 'skip, simple choosen').makeRecSkip(), handler(left, right, 'simple').makeRec2()),1)),
                     output(dataset([{'','Counter PROCESS: ' + name}], {'',string80 _Process})),
                     output(PROCESS(input(filters), initialRow, handler(left, right, 'counter').makeRecCtr(counter), handler(left, right, 'simple').makeRec2())),
                     output(choosen(PROCESS(input(filters), initialRow, handler(left, right, 'counter choosen').makeRecCtr(counter), handler(left, right, 'simple').makeRec2()),1)),
                     output(PROCESS(input(filters), initialRow, handler(left, right, 'skip, counter').makeRecCtrSkip(counter), handler(left, right, 'simple').makeRec2())),
                     output(choosen(PROCESS(input(filters), initialRow, handler(left, right, 'skip, counter choosen').makeRecCtrSkip(counter), handler(left, right, 'simple').makeRec2()),1))
)
ENDMACRO;


//#########################################
//All MACRO Calls 

simplePROCESS(straightiter,seed100,'straight');
simplePROCESS(straightitergrouped,group(seed100, value),'grouped');

simplePROCESS(straightiterfiltered,seed100,'filtered',value > 3);
simplePROCESS(straightitergroupedfiltered,group(seed100, value),'grouped filtered',value > 3);

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
