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

// derived from jholt20.eclxml
// A more complex variation on the child queries shared in multiple conditional contexts
// This even generates multiple child query calls within the same branch

i := index({unsigned f1}, {string f2}, 'i');

inRec := RECORD
    UNSIGNED id;
END;

outRec := RECORD
    STRING text;
END;

outRec t(inRec l) := TRANSFORM

    resolveA := i(f1 = l.id)[1].f2;
    resolveB := i(f1 = l.id*2)[1].f2;
    resolveC := i(f1 = l.id*3)[1].f2;


    SELF.text := CASE(l.id, 1 => resolveA,
                            2 => resolveB,
                            3 => resolveA+resolveB,
                            4 => resolveA+resolveC,
                            5 => resolveB+resolveC,
                            '');
END;

inDs := DATASET([1,2,3], {inRec});

output(PROJECT(inDs, t(LEFT)));
