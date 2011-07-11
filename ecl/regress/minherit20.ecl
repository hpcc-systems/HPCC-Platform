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

//A variation on local derivation (minherit4)
//Current problems:
//1) Bound parameter will be concrete not abstract
//2) Cloning and overriding would need to be handled in bind phase (ok if common functions extracted)
//3) Would need a no_param(x, concreteAtom) also bound when a param is bound (or possible concrete(param)), so that uses could be distringuished


m1 := MODULE,virtual
shared value1 := 1;
shared value2 := 1;
export f() := value1 * value2;
        END;


f(m1 mp) := function

    mBase :=    project(mp, m1);

    mLocal :=   module(mBase)
        shared value1 := 100;
                end;

    return mLocal.f();
end;


m4 := MODULE(m1)
shared value2 := 21;
        END;


output(f(m4));      // Expected 2100

