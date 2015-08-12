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

r := RECORD
 unsigned4 flags;
END;

ds := DATASET('test',r,FLAT);

// legal
r tranx0(r p1, r p2) :=
TRANSFORM
    SELF := p1;
END;

rst0 := ITERATE(ds,tranx0(LEFT,RIGHT));
//errors
rst0x := ITERATE(ds,tranx0(RIGHT,LEFT));
rst0y := ITERATE(ds,tranx0(LEFT));
rst0z := ITERATE(ds,tranx0(LEFT,RIGHT,RIGHT));
rst0w := ITERATE(ds,tranx0(ds,RIGHT));
rst0v := ITERATE(ds,tranx0(LEFT,LEFT));
rst0u := ITERATE(ds,tranx0(LEFT,ds));

// legal
r tranx1(r p1) :=
TRANSFORM
    SELF := p1;
END;

rst1 := ITERATE(ds,tranx1(LEFT));
// errors
rst1x := ITERATE(ds,tranx1(RIGHT));
rst1y := ITERATE(ds,tranx1(ds));
rst1z := ITERATE(ds,tranx1(LEFT,RIGHT));


// illegal
r tranx2(r p1,r p2, r p3) :=
TRANSFORM
    SELF := p1;
END;

rst2 := ITERATE(ds,tranx2(LEFT));
rst2x := ITERATE(ds,tranx2(LEFT,RIGHT,ds));


