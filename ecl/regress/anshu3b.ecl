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
// A variation on anshu3
// In this situation it is important that resolveA, resolveB etc. are not commoned up since they are smart stepped

i := STEPPED(index({unsigned f1, unsigned f2}, {string f3}, 'i'), f2);

inRec := RECORD
    UNSIGNED id;
END;

outRec := RECORD
    STRING text;
END;

outRec t(inRec l) := TRANSFORM

    resolveA := i(f1 = l.id);
    resolveB := i(f1 = l.id*2);
    resolveC := i(f1 = l.id*3);


    processed := CASE(l.id, 1 => resolveA[1].f3,
                            2 => resolveB[1].f3,
                            3 => JOIN([resolveA, resolveB], LEFT.f2 = RIGHT.f2, TRANSFORM(LEFT), SORTED(f2))[1].f3,
                            4 => JOIN([resolveA, resolveC], LEFT.f2 = RIGHT.f2, TRANSFORM(LEFT), SORTED(f2))[1].f3,
                            5 => JOIN([resolveB, resolveC], LEFT.f2 = RIGHT.f2, TRANSFORM(LEFT), SORTED(f2))[1].f3,
                            '');

    SELF.text := processed;
END;

inDs := DATASET([1,2,3], {inRec});

output(PROJECT(inDs, t(LEFT)));
