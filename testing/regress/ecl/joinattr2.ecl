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

rec :=
RECORD
        integer i;
        string1 id;
END;

 recout :=
RECORD
        string1 idL;
        string1 idR;
        unsigned cnt;
END;

 

ds1 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);
ds2 := DATASET([{1,'D'}], rec);
ds3 := DATASET([{1,'E'}, {1,'F'}, {1,'G'}], rec);
ds4 := DATASET([{1,'A'}], rec);
ds5 := DATASET([{1,'B'}], rec);
ds6 := DATASET([{1,'A'}, {1,'B'}], rec);
ds7 := DATASET([{1,'C'}, {1,'D'}, {1,'E'}], rec);
ds8 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);
ds9 := DATASET([{2,'A'}], rec);
ds10 := DATASET([{0,'A'}], rec);

recout trans(rec L, rec R, unsigned cnt) :=
TRANSFORM

            SELF.idl := L.id;
            SELF.idr := R.id;
            SELF.cnt := cnt;
END;

recout transkip(rec L, rec R, unsigned cnt) :=
TRANSFORM

            SELF.idl := if(L.id='A',skip,L.id);
            SELF.idr := if(R.id='A',skip,R.id);
            SELF.cnt := cnt;
END;

j1 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), KEEP(1), LEFT OUTER);
j2 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), ATMOST(1), LEFT OUTER);
j3 := JOIN(ds2, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j4 := JOIN(ds1, ds7, (LEFT.i = RIGHT.i), transkip(LEFT, RIGHT, COUNTER));
j5 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), LEFT OUTER);
j6 := JOIN(ds2, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j7 := JOIN(ds4, ds3, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT, COUNTER), KEEP(2));
j8 := JOIN(ds1, ds3, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), KEEP(2),LEFT OUTER);
j9 := JOIN(ds6, ds7, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), KEEP(5));
j10 := JOIN(ds6, ds7, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j11 := JOIN(ds1, ds7, (LEFT.i = RIGHT.i), transkip(LEFT, RIGHT, COUNTER));
j12 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j13 := JOIN(ds1, ds3, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j14 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT, COUNTER), KEEP(2));
j15 := JOIN(ds1, ds1, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), KEEP(1));
j16 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), KEEP(1));
j17 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), LOOKUP);
j18 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), ATMOST(1));
j19 := JOIN(ds7, ds6, (LEFT.i = RIGHT.i)and(LEFT.id<=RIGHT.id), trans(LEFT, RIGHT, COUNTER), LEFT OUTER, LOOKUP);
j20 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), KEEP(1), LEFT OUTER);
j21 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j22 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), ATMOST(1), LEFT OUTER);
j23 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j24 := JOIN(ds1, ds9, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT, COUNTER), KEEP(2));
j25 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), KEEP(1), LEFT OUTER);
j26 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j27 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), ATMOST(1), LEFT OUTER);
j28 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), RIGHT OUTER);
j29 := JOIN(ds9, ds1, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT, COUNTER), KEEP(2));
j30 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), KEEP(1), LEFT OUTER);
j31 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), LOOKUP);
j32 := JOIN(ds1, ds1, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), LOOKUP);
j33 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, trans(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j34 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j35 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT, COUNTER), LOOKUP);
j36 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j37 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j38 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), KEEP(1), LEFT OUTER);
j39 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), LOOKUP, LEFT OUTER);
j40 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), ATMOST(1), LEFT OUTER);

j41 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), MANY LOOKUP);
j42 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), MANY LOOKUP, KEEP(1));
j43 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), MANY LOOKUP, ATMOST(1));
j44 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT, COUNTER), MANY LOOKUP, LEFT OUTER);
j45 := JOIN(ds1, ds8, RIGHT.id<'B', trans(LEFT, RIGHT, COUNTER), ALL);
j46 := JOIN(ds1, ds8, RIGHT.id<'B', trans(LEFT, RIGHT, COUNTER), ALL, KEEP(1));
j47 := JOIN(ds1, ds8, RIGHT.id>'E', trans(LEFT, RIGHT, COUNTER), ALL, LEFT OUTER);
j48 := JOIN(ds1, ds8, LEFT.i = RIGHT.i AND RIGHT.id>'B', trans(LEFT, RIGHT, COUNTER), LOOKUP);
j49 := JOIN(ds9, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT, COUNTER), RIGHT ONLY);

sequential(
    output(j1),
    output(j2),
    output(j3),
    output(j4),
    output(j5),
    output(j6),
    output(j7),
    output(j8),
    output(j9),
    output(j10),
    output(j11),
    output(j12),
    output(j13),
    output(j14),
    output(j15),
    output(j16),
    output(j17),
    output(j18),
    output(j19),
    output(j20),
    output(j21),
    output(j22),
    output(j23),
    output(j24),
    output(j25),
    output(j26),
    output(j27),
    output(j28),
    output(j29),
    output(j30),
    output(j31),
    output(j32),
    output(j33),
    output(j34),
    output(j35),
    output(j36),
    output(j37),
    output(j38),
    output(j39),
    output(j40),
    output(j41),
    output(j42),
    output(j43),
    output(j44),
    output(j45),
    output(j46),
    output(j47),
    output(j48),
    output(j49),
);
