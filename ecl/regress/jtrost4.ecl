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
