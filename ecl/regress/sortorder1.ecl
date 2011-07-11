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
