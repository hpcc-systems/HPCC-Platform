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

int_a := interface
    export unsigned aa;
    export string ab;
end;

mod_a := module(int_a)
    export unsigned aa := 10;
    export string ab := 'HELLO';
end;

int_b := interface
    export unsigned aa;
    export string ab;
    export real bc;
end;

fun_c(int_b in_mod) := function
    return in_mod.aa * in_mod.bc;
end;

fun_d(int_a in_mod) := function
    tempmodf := module(project(in_mod, int_b, opt))
        export real bc := 5.0;
    end;
    return in_mod.ab + (string)fun_c(tempmodf);
end;

output(fun_d(mod_a));
