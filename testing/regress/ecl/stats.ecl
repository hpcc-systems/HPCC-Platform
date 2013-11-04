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

//normalizeFP 10

pointRec := { real x, real y };



analyse( ds) := macro

#uniquename(stats)
%stats% := table(ds, {  c := count(group),
                        sx := sum(group, x),
                        sy := sum(group, y),
                        sxx := sum(group, x * x),
                        sxy := sum(group, x * y),
                        syy := sum(group, y * y),
                        varx := round(variance(group, x),6);
                        vary := round(variance(group, y),6);
                        varxy := round(covariance(group, x, y),6);
                        rc := round(correlation(group, x, y),6) });

output(%stats%);

// Following should be varx, vary, varxy, and rc respectively
output(%stats%, { varx_check := round((sxx-sx*sx/c)/c,6),
                vary_check := round((syy-sy*sy/c)/c,6),
                varxy_check := round((sxy-sx*sy/c)/c,6),
                rc_check := round((varxy/sqrt(varx*vary)),6) });
                
                
output(%stats%, { best_fit := 'bestFit: y=' + (string)((sy-sx*varxy/varx)/c)+ ' + ' +(string)(varxy/varx)+'x' });

endmacro;

rx(real x) := (x + (random()-0x80000000)/0x100000000);
ds1 := dataset([{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}], pointRec);
ds2 := dataset([
{1.93896e+009,2.04482e+009},
{1.77971e+009,8.54858e+008},
{2.96181e+009,1.24848e+009},
{2.7744e+009,1.26357e+009},
{1.14416e+009,4.3429e+008},
{3.38728e+009,1.30238e+009},
{3.19538e+009,1.71177e+009}
], pointRec);
ds3 := dataset([
{1,1.00039},
{2,2.07702},
{3,2.86158},
{4,3.87114},
{5,5.12417},
{6,6.20283}
], pointRec);

analyse(ds1);
analyse(ds2);
analyse(ds3);
