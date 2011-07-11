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

inrec := RECORD
          unsigned6 did;
          unsigned6 rid;
          qstring18 vendor_id;
          integer4 dob;
          string1 jflag1;
          integer4 best_dob;
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
          unsigned3 dt_last_seen;
          integer1 good_nmaddr;
          unsigned1 rare_name;
          unsigned1 head_cnt;
          boolean prim_range_fraction;
         END;


sd15 := dataset('x', inrec, thor);



    outf1 := dedup(sort(distribute(group(sd15),hash(did)),did,local),all,local);

    slimrec := record
        outf1.did;
        outf1.lname;
    end;

    slim1 := table(outf1,slimrec,local);
    slim2 := dedup(sort(slim1,did,lname,local),did,lname,local);

    outf1 crosspop(outf1 L, slim2 R) := transform
        self.lname := R.lname;
        self := L;
    end;

    // create a copy of each full record with each of the unique lnames per did
    outf2 := join(outf1,slim2,left.did = right.did, crosspop(left,right),local);

    d := dedup(sort(outf2,record,local),local);
    d_pst := d;


output(d_pst);
