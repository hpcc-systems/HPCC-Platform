/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

