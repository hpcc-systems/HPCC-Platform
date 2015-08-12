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

fi := dataset('person', { unsigned8 person_id, string10 per_ssn, string8 per_dob, string99 per_first_name }, thor);

fg := group(fi,per_first_name,per_ssn,all);

fi i1(fi l,fi r) := transform
  self.per_dob := if ( r.per_dob = '' , l.per_dob, r.per_dob );
  self := r;
  end;

fs := sort(fg,-per_dob);
fit := iterate(fs,i1(left,right));

fd := dedup(fit,per_dob);
f := group(fd);

count(fi);
count(fg);
count(fs);
count(fit);
count(fd);
count(f);

output(f)