/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

gavLib := service
    set of integer4 getPrimes() : eclrtl,pure,library='eclrtl',entrypoint='rtlTestGetPrimes',oldSetFormat;
    set of integer4 getFibList(const set of integer4 inlist) : eclrtl,pure,library='eclrtl',entrypoint='rtlTestFibList',newset;
end;

r1 := record
unsigned4 id;
end;

r2 := record
unsigned4 id;
set of integer4 zips;
end;

d := dataset([{1},{2},{3},{4}], r1);

r2 t(r1 l) := transform
    SELF.id := l.id;
    self.zips := gavLib.getFibList([1,l.id]);
    end;

p := project(d, t(left));

f := p(id*2 in zips);

count(f);

r2 t2(r1 l) := transform
    SELF.id := l.id;
    self := [];
    end;

p2 := project(d, t2(left));

f2 := p2(id*2 in zips);

count(f2);


r2 t3(r1 l) := transform
    SELF.id := l.id;
    self.zips := [1,2,3,4];
    end;

p3 := project(d, t3(left));

f3 := p3(id*2 in zips);

output(f3, {id});

