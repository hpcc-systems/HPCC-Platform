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
//Error: assert(value || canOmit) failed - file: F:\build\533_01\project\newecl\ecl\hqlcpp\hqlttcpp.cpp, line 4636

EXPORT string8 Xyz__version_file := '20040629';

EXPORT Xyz__Layout_Xyz := 
 RECORD
  string10 pty_key;
  string60 source;
  string200 orig_pty_name;
  string200 orig_vessel_name;
  string50 addr_1;
  string50 addr_2;
  string50 addr_3;
  string50 addr_4;
  string50 addr_5;
  string50 addr_6;
  string50 addr_7;
  string50 addr_8;
  string50 addr_9;
  string50 addr_10;
  string75 remarks_1;
  string75 remarks_2;
  string75 remarks_3;
  string75 remarks_4;
  string75 remarks_5;
  string75 remarks_6;
  string75 remarks_7;
  string75 remarks_8;
  string75 remarks_9;
  string75 remarks_10;
  string75 remarks_11;
  string75 remarks_12;
  string75 remarks_13;
  string75 remarks_14;
  string75 remarks_15;
  string75 remarks_16;
  string75 remarks_17;
  string75 remarks_18;
  string75 remarks_19;
  string75 remarks_20;
  string75 remarks_21;
  string75 remarks_22;
  string75 remarks_23;
  string75 remarks_24;
  string75 remarks_25;
  string75 remarks_26;
  string75 remarks_27;
  string75 remarks_28;
  string75 remarks_29;
  string75 remarks_30;
  string200 cname;
  string5 title;
  string20 fname;
  string20 mname;
  string20 lname;
  string5 suffix;
  string3 a_score;
  string10 prim_range;
  string2 predir;
  string28 prim_name;
  string4 addr_suffix;
  string2 postdir;
  string10 unit_desig;
  string8 sec_range;
  string25 p_city_name;
  string25 v_city_name;
  string2 st;
  string5 zip;
  string4 zip4;
  string4 cart;
  string1 cr_sort_sz;
  string4 lot;
  string1 lot_order;
  string2 dpbc;
  string1 chk_digit;
  string2 record_type;
  string3 county;
  string10 geo_lat;
  string11 geo_long;
  string4 msa;
  string7 geo_blk;
  string1 geo_match;
  string4 err_stat;
 END;

EXPORT Xyz__File_Xyz := DATASET('~in::Xyz_file_' + Xyz__version_file, Xyz__Layout_Xyz, FLAT);

EXPORT pattern Text__Digit := PATTERN('[0-9]');

f := Xyz__File_Xyz;

r :=  RECORD
  string10 pty_key;
  string75 txt;
 END;

r take_rem(f le, unsigned integer8 cnt) := TRANSFORM
    SELF.txt := (CHOOSE(cnt, le.remarks_1, le.remarks_2, le.remarks_3, le.remarks_4, le.remarks_5, le.remarks_6, le.remarks_7, le.remarks_8, le.remarks_9, le.remarks_10, le.remarks_11, le.remarks_12, le.remarks_13, le.remarks_14, le.remarks_15, le.remarks_16, le.remarks_17, le.remarks_18, le.remarks_19, le.remarks_20, le.remarks_21, le.remarks_22, le.remarks_23, le.remarks_24, le.remarks_25, le.remarks_26, le.remarks_27, le.remarks_28, le.remarks_29, le.remarks_30));
    SELF := le;
    END;

n := NORMALIZE(f, 30, take_rem(LEFT, COUNTER))(txt != '                                                                           ');

pattern n2 := Text__Digit Text__Digit?;

pattern date1 := n2 '/' n2 '/' n2 n2;

p := PARSE(n, txt, date1, r, first);

EXPORT Xyz__Comments_Fields := p;

OUTPUT(CHOOSEN(Xyz__Comments_Fields, 100));

