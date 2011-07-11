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

//C++ file failed to generate for ECL:
//Error: Could not find dataset aggregate(aggregate(GROUP(SORT(DISTRIBUTE(NEWTABLE(NEWTABLE(CHOOSEN(DISTRIBUTED(<table>(...

EXPORT zh__Layout_zh := 
 RECORD
  unsigned integer6 xyz := 0;
  unsigned integer6 rid;
  string1 pflag1 := '';
  string1 pflag2 := '';
  string1 pflag3 := '';
  string2 src;
  unsigned integer3 dt_first_seen;
  unsigned integer3 dt_last_seen;
  unsigned integer3 dt_vendor_last_reported;
  unsigned integer3 dt_vendor_first_reported;
  unsigned integer3 dt_nonglb_last_seen;
  string1 rec_type;
  qstring18 vendor_id;
  qstring10 phone;
  integer4 dob;
  qstring5 title;
  qstring20 fname;
  qstring20 mname;
  qstring20 lname;
  qstring5 name_suffix;
  qstring10 prim_range;
  string2 predir;
  qstring28 prim_name;
  qstring4 suffix;
  string2 postdir;
  qstring10 unit_desig;
  qstring8 sec_range;
  qstring25 city_name;
  string2 st;
  qstring5 zip;
  qstring4 zip4;
  string3 county;
  qstring4 msa;
  string1 TNT := ' ';
  string1 jflag1 := '';
  string1 jflag2 := '';
  string1 jflag3 := '';
 END;

f_h := DATASET('~20031205_1', zh__Layout_zh, FLAT);

EXPORT zh__File_zhs := DISTRIBUTE(f_h, HASH(xyz)) : PERSIST('~20031205', '400way');

EXPORT unsigned integer6 Jingo__Dummy_Base := 100000000000;

df := CHOOSEN(zh__File_zhs, 100);

xyzrec := 
{ unsigned integer6 xyz := RANDOM(), unsigned integer1 score := RANDOM() % 100 };

df2 := TABLE(df, xyzrec);

__15801__ := 
{ df2.xyz };

__15802__ := TABLE(df2, __15801__);

__15803__ := 
{ __15802__.xyz, integer8 xyz_count := COUNT(group), unsigned integer1 xyz_type := MAP(__15802__.xyz = 0 => 0,
     __15802__.xyz < Jingo__Dummy_Base => 1,
     2) };

__15804__ := TABLE(DISTRIBUTE(__15802__, HASH(xyz)), __15803__, xyz);

__15805__ := 
{ __15804__.xyz_count, __15804__.xyz_type, integer8 xyz_freq := COUNT(group) };

__15806__ := TABLE(__15804__, __15805__, xyz_count, xyz_type);

OUTPUT(SUM(__15806__, xyz_freq * xyz_count), named('Stats1'));
OUTPUT(SUM(__15806__(xyz_type = 1), xyz_freq * xyz_count), named('Stats2'));
OUTPUT(SUM(__15806__(xyz_type = 2), xyz_freq * xyz_count), named('Stats3'));
OUTPUT(COUNT(__15804__(xyz_type = 1)), named('Stats4'));
OUTPUT(COUNT(__15804__(xyz_type = 2)), named('Stats5'));
OUTPUT(COUNT(__15804__(xyz_type != 0)), named('Stats6'));
OUTPUT(CHOOSEN(__15806__(xyz_type = 1), ALL), , named('Frequency_Count'));
OUTPUT(CHOOSEN(__15806__(xyz_type = 2), ALL), , named('Local_Frequency_Count'));
