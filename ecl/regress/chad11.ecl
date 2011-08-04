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

export x := SERVICE
 integer x(integer a) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;


integer percentage_diff(integer a, integer b) := x.x((b-a)/a);


old := dataset('old', { unsigned did; }, thor);

tot_recs := count(old);

diff_rec := record
    integer8 total_records := tot_recs;
    integer8 any_change := count(group);
    integer8 percent_change := percentage_diff(tot_recs, tot_recs + count(group));
end;

did_diff := old;
change_counts := table(did_diff, diff_rec);
output(change_counts)
