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

Layout_inner :=
RECORD
    INTEGER i;
END;

Layout_outer :=
RECORD
    INTEGER seq;
    dataset(Layout_inner) is;
END;

Layout_in :=
RECORD
    INTEGER seq;
    INTEGER i;
END;

d_in := dataset([{1, 1}, {1, 2}, {1, 3}, {2, 1}, {3, 2}, {3, 3}], Layout_in);
output(d_in);

// We want to denormalize some incoming dataset into a
// sequence number and child records
blank_inner := dataset([], Layout_inner);

// first project into Layout_outer
Layout_outer to_outer(Layout_in le) :=
TRANSFORM
    SELF.seq := le.seq;
    SELF.is := blank_inner;
END;

p := dedup(PROJECT(d_in, to_outer(LEFT)), seq, ALL);

// then denorm
Layout_outer denorm(Layout_outer le, Layout_in ri, INTEGER c) :=
TRANSFORM
    SELF.seq := le.seq;
    SELF.is := le.is + ROW({ri.i}, layout_inner);
END;
d := DENORMALIZE(p, d_in, LEFT.seq = RIGHT.seq, denorm(LEFT, RIGHT, COUNTER));

output(d);
