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
