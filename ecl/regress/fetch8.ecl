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

string2 x1 := '' : stored('x1');
boolean x2 := false : stored('x1');

//xName := if(x1='on', 'x', 'y');
xName := if(x2, 'x', 'y');

d := dataset('~local::rkc::person'+xName, { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);

i := index(d, { string10 zname, string15 f_name := name, unsigned8 fpos{virtual(fileposition)} } , '~local::key::person');

//ii := choosen(i(f_name NOT IN ['RICHARD','SARAH'] or f_name between 'GAVIN' AND 'GILBERT'), 10);

myFilter := i.f_name between 'GAVIN' AND 'GILBERT';
ii := choosen(i((f_name[1]!='!') AND (myFilter or f_name[1..2]='XX' or 'ZZZ' = f_name[..3])), 10);      //should be or but not processed yet.

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

z := fetch(d, ii, RIGHT.fpos);//, xt(LEFT, RIGHT));
output(z);