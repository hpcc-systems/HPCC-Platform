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

rec :=  RECORD
  unsigned integer6 did;
  qstring18 vendor_id;
  qstring9 ssn;
  integer4 dob;
  qstring10 phone;
  qstring20 fname;
  qstring20 lname;
  qstring20 mname;
  qstring5 name_suffix;
  qstring10 prim_range;
  qstring28 prim_name;
  qstring5 zip;
  string2 st;
  qstring25 city_name;
  qstring8 sec_range;
  unsigned integer3 dt_last_seen;
  boolean good_ssn := false;
  integer1 good_nmaddr := 0;
  unsigned integer1 rare_name := 255;
 END;

outf0 := dataset('x', rec, thor);
outf2 := distribute(outf0, hash(did));

outf := DEDUP(SORT(outf2, did, vendor_id, ssn, dob, phone, fname, lname, mname, name_suffix, prim_range, prim_name, zip, st, city_name, sec_range, dt_last_seen, good_ssn, good_nmaddr, rare_name, local), local);

h0 := outf(fname != '                    ', lname != '                    ', zip != '     ');

h := GROUP(SORT(outf, did, local), did, local);

output(h);
