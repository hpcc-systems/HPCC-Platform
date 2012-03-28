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

//alias' = if (l.f1 = 1 || l.f2 == 1, alias1, []);
workRecord t1(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    //Same no_mapto, different case
    SELF.f1 := case(l.f1, 2=>99, alias1=>alias1, 56);
    SELF := [];
END;


//alias' = if (l.f1 = 1 || l.f2 == 1, alias1, []);
workRecord t2(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    //Same no_mapto, different case
    SELF.f1 := case(l.f1, 2=>99, alias1=>alias1, 56);
    SELF.f2 := case(l.f2, 2=>98, alias1=>alias1, 55);
    SELF := [];
END;


SEQUENTIAL(
    OUTPUT(PROJECT(ds, t1(LEFT))),
    OUTPUT(PROJECT(ds, t2(LEFT))),
    'Done'
);
