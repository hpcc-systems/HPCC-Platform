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

#option ('globalFold', false);
#option ('targetClusterType', 'hthor');
#option ('pickBestEngine', false);

d := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);
d2 := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, csv);

i := index(d, { string10 zname, string15 f_name := name, unsigned8 fpos{virtual(fileposition)} } , '~local::key::person');

//ii := choosen(i(f_name NOT IN ['RICHARD','SARAH'] or f_name between 'GAVIN' AND 'GILBERT'), 10);

dd := record
  string15 lname;
  string15 rname;
  unsigned8 lpos;
  unsigned8 rpos;
end;

dd xt(D l, i r) := TRANSFORM
sELF.lname := l.name;
sELF.rname := r.f_name;
sELF.lpos := l.dpos;
sELF.rpos := r.fpos;
END;

x1 := i(f_name <> 'x');
output(x1,,'a');

w1 := i(f_name <> 'x');
w2 := limit(w1, 100);
output(w2);

y1 := limit(i, 200);
y2 := y1(f_name <> 'x');
output(y2);

z1 := i(f_name <> 'x');
z2 := limit(z1, 200);
z3 := z2(f_name <> 'y');
output(z3);


e1 := i(keyed(f_name <> 'e'));
e2 := limit(e1, 200, keyed, count);
e3 := e2(keyed((integer)zname *3 > 9));
e4 := limit(e3, 2000);
e5 := e4(zname <> '');
output(e5);
