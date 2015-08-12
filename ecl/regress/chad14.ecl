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

x_rec       :=  record
string12            did;
                end;

fh_slim_rec := record
    unsigned6   did;
    string5     title;
    string20    fname;
    string20    mname;
    string20    lname;
    string5     name_suffix;
    string8     dob;
end;

f := dataset('~thor::genalytics_owner_props_2', fh_slim_rec, flat);

give2(real8 a) := round(a * 100) / 100;

rec := record
    D1 := give2(ave(group, if(f.did > 0, 100, 0)));
//  D1 := give2(ave(group, if(f.spd1.did > 0, 100, 0)));
//  D2 := give2(ave(group, if(f.spd2.did > 0, 100, 0)));

//  A1 := give2(ave(group, if(f.spa1.did > 0, 100, 0)));
//  A2 := give2(ave(group, if(f.spa2.did > 0, 100, 0)));
end;

ft := table(f, rec);
output(ft)
