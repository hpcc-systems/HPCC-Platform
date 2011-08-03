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

arec :=
            RECORD
unsigned1       a;
unsigned1       b;
unsigned4       sequence := 0;
unsigned4       node := 0;
            END;

alpha := nofold(dataset([
        {1,1},
        {1,2}
        ], arec));

beta := nofold(dataset([
        {2,2},
        {3,2}
        ], arec));

/*
alpha := dataset('alpha', arec, thor);
beta := dataset('beta', arec, thor);
*/
gamma := alpha(a >= 0);
x := if(count(gamma) > 1, alpha, beta);
output(x);