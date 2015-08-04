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

export Key_Search := index(st,{t},'~thor::searchkey');

buildindex(Key_Search)
