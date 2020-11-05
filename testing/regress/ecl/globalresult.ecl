/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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


//Ensure that references to temporaries outside of the graph are handled correctly
ds := DATASET(10, transform({unsigned id}, SELF.id := COUNTER)) : GLOBAL(FEW);
o1 := output(NOTHOR(count(ds)));


idRec := {unsigned id};
ds2base := DATASET(10, transform(idRec, SELF.id := COUNTER));

//ds2 creates a global workunit write, which is then no longer needed once the child query is optimized.
ds2 := ds2base : GLOBAL(FEW);


ds3 := DATASET(20, transform({unsigned x, unsigned y}, SELF.x := COUNTER, SELF.y := 20));


dsx(unsigned x) := DATASET(30, transform({DATASET(idRec) f1, unsigned f2}, SELF.f1 := ds2, SELF.f2 := x + COUNTER));

ds3 t(ds3 l) := TRANSFORM
    SELF.x := l.x;
    SELF.y := COUNT(dsx(l.y));
END;

ds4 := project(nocombine(ds3), t(LEFT));

sequential(
    o1,
    parallel(
        output(ds4);
        output(ds2base);
    )
);
