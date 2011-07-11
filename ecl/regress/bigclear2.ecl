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


r1 :=        RECORD
string20000     f1;
data20000       f2;
string20000     f3;
data20000       f4;
string4000      fa1;
data4000        fa2;
string4000      fa3;
data4000        fa4;
string4000      fb1;
data4000        fb2;
string4000      fb3;
data4000        fb4;
string4000      fc1;
data4000        fc2;
string4000      fc3;
data4000        fc4;
string4000      fd1;
data4000        fd2;
string4000      fd3;
data4000        fd4;
string4000      fe1;
data4000        fe2;
string4000      fe3;
data4000        fe4;
            END;

r1 t(r1 l, r1 r) := transform
    self := r;
    END;

d := dataset('d', r1, thor);

output(ITERATE(d,t(LEFT,RIGHT)));



