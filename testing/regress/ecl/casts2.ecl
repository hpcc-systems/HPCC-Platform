/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

// Test some casts to varstring etc

r := record
  integer i;
  utf8 s;
  varunicode vu;
  varunicode vu5;
end;

r1 := record
  varstring5 s1;
  varstring5 s2;
  varunicode5 u1;
  varunicode5 u2;
  data d;
  data5 d5;
  data d1;
  data5 d15;
end;

d := dataset([{1,'d', u'€', u'€'}], r);

r1 t(d l) := transform
  self.s1 := (varstring5) l.i;
  self.s2 := (varstring5) l.s;
  self.u1 := (varunicode5) l.i;
  self.u2 := (varunicode5) l.s;
  self.d := (data) l.vu;
  self.d5 := (data5) l.vu;
  self.d1 := (data) l.vu5;
  self.d15 := (data5) l.vu5;
end;

project(d, t(left));
project(nofold(d), t(left));
