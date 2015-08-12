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

#option ('targetClusterType', 'thor');

pointRec := { real x, real y };

ds := dataset('points', pointRec, thor);

stats := table(ds, {    c := count(group),
                        sx := sum(group, x),
                        sy := sum(group, y),
                        sxx := sum(group, x * x),
                        sxy := sum(group, x * y),
                        syy := sum(group, y * y),
                        varx := variance(group, x);
                        vary := variance(group, y);
                        varxy := covariance(group, x, y);
                        mny := max(group, y),
                        mxy := min(group, y),
                        e := exists(group),
                        ey2 := exists(group, y > 2),
                        eny2 := not exists(group, y > 2),
                        ay := ave(group, y),
                        rc := correlation(group, x, y) }, x, merge);

output(stats);


stats2 := table(ds, {   c := count(group),
                        sx := sum(group, x),
                        sy := sum(group, y),
                        sxx := sum(group, x * x) }, y, merge);

output(stats2);
