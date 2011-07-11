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

#option ('optimizeGraph', false);
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

