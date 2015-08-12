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

#option ('optimizeGraph', false);
#option ('globalFold', false);
string15 name1 := 'LOR' : stored('LORRAINE');

d := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);

i := sorted(index(d, { string15 f_name := name, unsigned8 fpos } , 'key::person'));

//ii := choosen(i(f_name>='RICHARD', f_name <= 'SARAH', f_name <>'ROBERT', f_name[1]='O'), 10);
ii := i(f_name IN ['RICHARD A']);

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

d xt1(D l) := TRANSFORM
sELF.name := 'f';
SELF := l;
END;

x := fetch(d, ii, RIGHT.fpos, xt(LEFT, RIGHT));
output(choosen(x, 1000000));
output(choosen(sort(i(f_name >= name1), f_name), 10));
//output(choosen(sort(i(f_name >= 'LOR'), f_name), 10));
//output(project(d(name='RIC'), xt1(LEFT)));
//count(d(name='RIC'));
