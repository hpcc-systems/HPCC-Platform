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

#option ('foldAssign', false);
#option ('globalFold', false);
import ut;

gr := record
  string2 st := '';
  qstring25 v_city_name := '';
  qstring20 name_last := '';
  qstring20 listed_name := '';
  qstring10 prim_range := '';
  qstring28 prim_name := '';
  string5 z5 := '';
  qstring20 name_first := '';
  string10 phone10 := '';
  end;
  
g := dataset('g', gr, thor);

sg := record
  string2 st := '';
  qstring25 v_city_name := '';
  qstring20 name_last := '';
  qstring10 prim_range := '';
  qstring28 prim_name := '';
  unsigned3 z5 := 0;
  qstring20 name_first := '';
  unsigned8 phone := 0;
  end;
  
sg into(g le) := transform
  self.name_last := if ( le.name_last <> '', le.name_last, ut.word(le.listed_name,1));
  self.name_first := if ( le.name_first <> '', le.name_first, ut.word(le.listed_name,2));
  self.z5 := (unsigned4)le.z5;
  self.phone := (unsigned8)le.phone10;
  self := le;
  end;  

t := project(g(st<>'',v_city_name<>'',phone10<>''),into(left));

st := sort(t(name_last<>''),whole record);

export Key_Gong_Search := index(st,,'~thor::gong_searchkey');

buildindex(Key_Gong_Search)
