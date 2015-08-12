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
