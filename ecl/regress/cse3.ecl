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

potentialAlias(unsigned v1, unsigned v2) := (sqrt(v1 * v2));
unsigned forceAlias(unsigned v1, unsigned v2) := potentialAlias(v1, v2) * potentialAlias(v1, v2);

workRecord :=
            RECORD
unsigned        f1;
unsigned        f2;
unsigned        f3;
unsigned        f4;
unsigned        f5;
unsigned        f6;
unsigned        f7;
unsigned        f8;
unsigned        f9;
            END;

ds := dataset('ds', workRecord, thor);

//if (L.f1, alias)
workRecord t1(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    SELF.f1 := IF(l.f1=0, alias1, 0);
    SELF := [];
END;

//alias; if (L.f1, get-alias)
workRecord t2(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    SELF.f1 := alias1 * IF(l.f1=0, alias1, 0);
    SELF := [];
END;

//alias' = if(l.f1 || l.f2, alias1); if (L.f1, get-alias') * IF(L.f2, get-alias')
workRecord t3(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    SELF.f1 := IF(l.f1=0, alias1, 1) * IF(l.f2 = 0, alias1, 1);
    SELF := [];
END;

//IF(L.f2, ...case2...)
workRecord t4(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    SELF.f1 := IF(l.f2 = 10, alias1 * IF(l.f1=0, alias1, 0), 1);
    SELF := [];
END;

//alias' = IF(L.f2, ...case3...)
workRecord t5(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    SELF.f1 := IF(l.f2= 10, IF(l.f1=0, alias1, 1) * IF(l.f2 = 0, alias1, 1), 1);
    SELF := [];
END;

//Same as t5, hopefully conditions are commoned up
workRecord t6(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    alias2 := forceAlias(l.f1, 2);
    SELF.f1 := IF(l.f2= 10, IF(l.f1=0, alias1, 1) * IF(l.f2 = 0, alias1, 1), 1);
    SELF.f2 := IF(l.f2= 10, IF(l.f1=0, alias2, 1) * IF(l.f2 = 0, alias2, 1), 1);
    SELF := [];
END;

//alias' = IF(L.f2, ...case3...)
workRecord t7(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    alias2 := forceAlias(l.f1, 2);
    alias3 := forceAlias(l.f1, 3);
    SELF.f1 := IF(l.f2= 10, IF(l.f1=alias1, alias2, 1) * IF(l.f2 = alias1, alias3, 1), 1);
    SELF := [];
END;

/*
if (l.f4) {
  c2 = ...;
  alias2' = if(c2, alias2, 0);
  c1' = if(c2, c1, false);
  alias' = IF((c1' && alias2') || c2, alias2')
  SELF.f1 := IF(c1', IF(alias2', alias1', 101), 102) * IF(c2, alias1', 103);

Note need to create aliases for c1, c2
*/
workRecord t8(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    alias2 := forceAlias(l.f1, 2);
    c1 := l.f2 = 100;
    c2 := l.f3 = 10;
    a1 := IF(alias2>91, alias1, 101);
    a2 := IF(c1, a1, 102);
    a3 := IF(c2, alias1, 103);
    a4 := a2 * a3;
    SELF.f1 := IF(l.f4 = 10, a4, 104);
    SELF := [];
END;

/*
if (l.f4) {
  c2 = ...
  alias2' = c2 ? alias2 : 0;
  c1' = c2 ? c1 : false;
  alias' = (c1' && alias2'>91) || c2, ? alias2 : 0;
  self->f1 := (c1' ? (alias2' ? alias1' : 101) : 102) * (c2 ? alias1' : 103);
}
else
  self->f1 = 104;

Note need to create aliases for c1, c2
*/
workRecord t9(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    alias2 := forceAlias(l.f1, 2);
    c1 := potentialAlias(l.f2, 3) = 100;
    c2 := potentialAlias(l.f3, 4) = 10;
    a1 := IF(alias2>91, alias1, 101);
    a2 := IF(c1, a1, 102);
    a3 := IF(c2, alias1, 103);
    a4 := a2 * a3;
    SELF.f1 := IF(l.f4 = 10, a4, 104);
    SELF := [];
END;

SEQUENTIAL(
    OUTPUT(PROJECT(ds, t1(LEFT))),
    OUTPUT(PROJECT(ds, t2(LEFT))),
    OUTPUT(PROJECT(ds, t3(LEFT))),
    OUTPUT(PROJECT(ds, t4(LEFT))),
    OUTPUT(PROJECT(ds, t5(LEFT))),
    OUTPUT(PROJECT(ds, t6(LEFT))),
    OUTPUT(PROJECT(ds, t7(LEFT))),
    OUTPUT(PROJECT(ds, t8(LEFT))),
    OUTPUT(PROJECT(ds, t9(LEFT))),
    'Done'
);
