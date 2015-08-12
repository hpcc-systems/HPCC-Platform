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
