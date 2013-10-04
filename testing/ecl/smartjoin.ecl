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

rec := RECORD
        integer i;
        string1 id;
END;

recout := RECORD
        integer i;
        string idL;
        string idR;
END;

recout createOut(integer i, string idL, string idR) := TRANSFORM
            SELF.i := i;
            SELF.idl := idL;
            SELF.idr := idR;
END;

ds1 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}, {2,'X'}, {2,'Y'}], rec);
ds2 := DATASET([{1,'a'}, {1,'b'}, {1,'c'}, {2,'x'}, {2,'y'}], rec);

recout trans(rec L, rec R) := createOut(L.i, L.id, R.id);

gr1 := GROUP(ds1, i, id);
j1 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), SMART);
j2 := JOIN(gr1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), SMART);
j3 := JOIN(gr1, gr1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), SMART);

p1 := PROJECT(ds1, createOut(LEFT.i, LEFT.id, ''));
gp1 := GROUP(p1, i, idL);

recout tr2(recout L, rec R) := TRANSFORM
            SELF.idR := L.idR + R.id;
            SELF := L;
END;

recout tr3(recout L, recout R) := TRANSFORM
            SELF.idR := L.idR + R.idL;
            SELF := L;
END;

d1 := DENORMALIZE(p1, ds2, LEFT.i = RIGHT.i, tr2(LEFT, RIGHT), SMART);
d2 := DENORMALIZE(gp1, ds2, LEFT.i = RIGHT.i, tr2(LEFT, RIGHT), SMART);
d3 := DENORMALIZE(gp1, gp1, LEFT.i = RIGHT.i, tr3(LEFT, RIGHT), SMART);

strRec := { string text };

recout trg(rec L, dataset(rec) Rs) := TRANSFORM
            SELF.i := L.i;
            SELF.idL := L.id;
            SELF.idR := AGGREGATE(Rs, strRec, TRANSFORM(strRec, SELF.text := RIGHT.text + LEFT.id))[1].text;
END;

dg1 := DENORMALIZE(ds1, ds2, LEFT.i = RIGHT.i, GROUP, trg(LEFT, ROWS(RIGHT)), SMART);
dg2 := DENORMALIZE(gr1, ds2, LEFT.i = RIGHT.i, GROUP, trg(LEFT, ROWS(RIGHT)), SMART);
dg3 := DENORMALIZE(gr1, gr1, LEFT.i = RIGHT.i, GROUP, trg(LEFT, ROWS(RIGHT)), SMART);

sequential(
    output(SORT(j1, i)),
    output(SORT(TABLE(j2, { cnt := COUNT(GROUP) }), cnt)),  // check output is not grouped
    output(SORT(j3, i)),
    output(SORT(d1, i)),
    output(SORT(TABLE(d2, { cnt := COUNT(GROUP) }), cnt)),  // check output is not grouped
    output(SORT(d3, i)),
    output(SORT(dg1, i)),
    output(SORT(TABLE(dg2, { cnt := COUNT(GROUP) }), cnt)),  // check output is not grouped
    output(SORT(dg3, i)),
);
