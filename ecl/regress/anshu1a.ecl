/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

// derived from vlad4size.eclxml
// A variation on anshu1.ecl, forcing getRow() into a child query, and
// generating simpler code in the process.

r1 := RECORD
    unsigned f1;
    unsigned f2;
    unsigned f3;
END;

r2 := RECORD
    unsigned whichItem;
    DATASET(r1) child;
END;

rIn := RECORD
    unsigned whichItem;
END;

r1 mkA() := TRANSFORM
    SELF.f1 := 1;
    SELF.f2 := 2;
    SELF.f3 := 3;
END;

r1 mkB() := TRANSFORM
    SELF.f1 := 10;
    SELF.f2 := 20;
    SELF.f3 := 30;
END;

r1 mkC() := TRANSFORM
    SELF.f1 := 100;
    SELF.f2 := 200;
    SELF.f3 := 300;
END;


r2 createChild(rIn l) := TRANSFORM
    SELF.whichItem := l.whichItem;
    SELF.child := MAP(l.whichItem = 1 => DATASET([mkA()]),
                      l.whichItem = 2 => DATASET([mkB()]),
                      l.whichItem = 3 => DATASET([mkC()]));
END;


extractChildren(DATASET(rIn) input) := FUNCTION

        settings := SORT(PROJECT(input,createChild(LEFT)),whichItem);

        RETURN NORMALIZE(settings,LEFT.child,TRANSFORM(RIGHT));
END;


searchRec := RECORD
    unsigned mx;
    r1 child;
END;


zRec := RECORD
    unsigned z;
END;


zRec tz(zRec l) := TRANSFORM

    dsIn := DATASET([l.z], rIn);
    selected := extractChildren(dsIn)[1];


    zRec mkA() := TRANSFORM
        SELF.z := selected.f1;
    END;

    zRec mkB() := TRANSFORM
        SELF.z := selected.f2;
    END;

    zRec mkC() := TRANSFORM
        SELF.z := selected.f3;
    END;

    getRow() := SORT(NOFOLD(MAP(l.z = 1 => DATASET([mkA()]),
                    l.z = 2 => DATASET([mkB()]),
                    l.z = 3 => DATASET([mkC()]))),z);

    SELF.z := getRow()[1].z;
END;


zDs := DATASET([1,2,3], {zRec});

output(PROJECT(zDs, tz(LEFT)));
