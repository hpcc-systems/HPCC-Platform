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