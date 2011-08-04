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

h := dataset('~local::rkc::person', { string1 fname,
string1 lname,
string1 mname,
string1 prim_range,
string1 prim_name,
string1 zip,
string1 did,
string1 sec_range,
string1 name_suffix,
string6 filler
}, thor, opt);

t := h; //table(h,layout_name_address);

dis_t :=  group(
            sort(
              distribute(t,hash(fname,lname)),
              fname,
              lname,
              prim_range,
              prim_name,
              zip,
              local ),
              fname,
              lname,
              prim_range,
              prim_name,
              zip,
              local );

dt := dedup( sort(dis_t,mname,name_suffix,sec_range,did ),mname,name_suffix,sec_range,did );

dtg := sort(dt,did,sec_range);
dc1 := dedup( dtg,did );

add_rec := record
  dt.fname;
  dt.lname;
  dt.prim_range;
  dt.prim_name;
  dt.zip;
  unsigned4 cnt := count(group);
  end;

add_count := table(group(dc1),add_rec,fname,lname,prim_range,prim_name,zip);

output(add_count);

add_count_local := table(group(dc1),add_rec,fname,lname,prim_range,prim_name,zip,local);

output(add_count_local);
