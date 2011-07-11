/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

recout trans(rec L, rec R) :=
TRANSFORM

            SELF.idl := L.id;
            SELF.idr := R.id;
END;

recout transkip(rec L, rec R) :=
TRANSFORM

            SELF.idl := if(L.id='A',skip,L.id);
            SELF.idr := if(R.id='A',skip,R.id);
END;

j1 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTLEFT);
j2 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), LEFT OUTER);
j3 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), ATMOST(1), LEFT OUTER);
j4 := JOIN(ds2, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTRIGHT);
j5 := JOIN(ds2, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), RIGHT OUTER);
j6 := JOIN(ds1, ds7, (LEFT.i = RIGHT.i), transkip(LEFT, RIGHT));
j7 := JOIN(ds1, ds2, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), LEFT OUTER);
j8 := JOIN(ds2, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), RIGHT OUTER);
j9 := JOIN(ds4, ds3, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT), KEEP(2));
j10 := JOIN(ds1, ds5, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), FIRSTLEFT);
j11 := JOIN(ds1, ds5, (LEFT.i = RIGHT.i)and(LEFT.id>=RIGHT.id), trans(LEFT, RIGHT), FIRSTLEFT);
j12 := JOIN(ds1, ds3, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), KEEP(2),LEFT OUTER);
j13 := JOIN(ds6, ds7, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), KEEP(5));
j14 := JOIN(ds6, ds7, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), KEEP(1),RIGHT OUTER);
j15 := JOIN(ds1, ds7, (LEFT.i = RIGHT.i), transkip(LEFT, RIGHT));
j16 := JOIN(ds1, ds5, (LEFT.i = RIGHT.i)and(LEFT.id>=RIGHT.id), trans(LEFT, RIGHT), FIRST);
j17 := JOIN(ds1, ds3, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1),FULL OUTER);
j18 := JOIN(ds1, ds3, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1),FULL ONLY);
j19 := JOIN(ds1, ds3, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1),RIGHT OUTER);
j20 := JOIN(ds1, ds3, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1),RIGHT ONLY);
j21 := JOIN(ds7, ds1, (LEFT.i = RIGHT.i)and(LEFT.id<=RIGHT.id), trans(LEFT, RIGHT), FIRST);
j22 := JOIN(ds1, ds1, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), KEEP(1));
j23 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), KEEP(1));
j24 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), LOOKUP);
j25 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), ATMOST(1));
j26 := JOIN(ds1, ds1, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), FIRST);
j27 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i), trans(LEFT, RIGHT), FIRST);
j28 := JOIN(ds7, ds1, (LEFT.i = RIGHT.i)and(LEFT.id<=RIGHT.id), trans(LEFT, RIGHT), LEFT OUTER, LOOKUP);
j29 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTLEFT);
j30 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), LEFT OUTER);
j31 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j32 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), ATMOST(1), LEFT OUTER);
j33 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTRIGHT);
j34 := JOIN(ds1, ds9, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), RIGHT OUTER);
j35 := JOIN(ds1, ds9, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT), KEEP(2));
j36 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTLEFT);
j37 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), LEFT OUTER);
j38 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j39 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), ATMOST(1), LEFT OUTER);
j40 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), FIRSTRIGHT);
j41 := JOIN(ds9, ds1, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), KEEP(1), RIGHT OUTER);
j42 := JOIN(ds9, ds1, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), trans(LEFT, RIGHT), KEEP(2));
j43 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), FIRSTLEFT);
j44 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), KEEP(1), LEFT OUTER);
j45 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), LOOKUP);
j46 := JOIN(ds1, ds1, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), LOOKUP);
j47 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, trans(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j48 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j49 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT), LOOKUP);
j50 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j51 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j52 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), FIRSTLEFT);
j53 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), KEEP(1), LEFT OUTER);
j54 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), LOOKUP, LEFT OUTER);
j55 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), ATMOST(1), LEFT OUTER);
j56 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), FIRSTRIGHT);
j57 := JOIN(ds1, ds8, LEFT.i = RIGHT.i, transkip(LEFT, RIGHT), KEEP(1), RIGHT OUTER);
j58 := JOIN(ds1, ds8, (LEFT.i = RIGHT.i)and(RIGHT.id>'E'), transkip(LEFT, RIGHT), KEEP(2));

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
    output(j50),
    output(j51),
    output(j52),
    output(j53),
    output(j54),
    output(j55),
    output(j56),
    output(j57),
    output(j58)
);

