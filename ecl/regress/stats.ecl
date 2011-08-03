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



pointRec := { real x, real y };



analyse( ds) := macro

#uniquename(stats)
%stats% := table(ds, {  c := count(group),
                        sx := sum(group, x),
                        sy := sum(group, y),
                        sxx := sum(group, x * x),
                        sxy := sum(group, x * y),
                        syy := sum(group, y * y),
                        varx := variance(group, x);
                        vary := variance(group, y);
                        varxy := covariance(group, x, y);
                        rc := correlation(group, x, y) });

output(%stats%);

// Following pairs should all match
output(%stats%, { varx, (sxx-sx*sx/c)/c,
                vary, (syy-sy*sy/c)/c,
                varxy, (sxy-sx*sy/c)/c,
                rc, (varxy/sqrt(varx*vary)) });


output(%stats%, { 'bestFit: y=' + (string)((sy-sx*varxy/varx)/c)+ ' + ' +(string)(varxy/varx)+'x' });

endmacro;

rx(real x) := (x + (random()-0x80000000)/0x100000000);
ds1 := dataset([{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}], pointRec);
ds2 := dataset([{random(),random()},{random(),random()},{random(),random()},{random(),random()},{random(),random()},{random(),random()},{random(),random()}], pointRec);
ds3 := dataset([{1,rx(1)},{2,rx(2)},{3,rx(3)},{4,rx(4)},{5,rx(5)},{6,rx(6)}], pointRec);

analyse(ds1);
analyse(ds2);
analyse(ds3);

