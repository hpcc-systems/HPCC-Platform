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
