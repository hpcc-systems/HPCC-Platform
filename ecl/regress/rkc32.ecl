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

d := dataset([{1}, {2}], {unsigned f});

string tostr(unsigned i) := CASE(i, 1=>'one', 2=>'two', 'lots');

boolean stringthem := false : stored('stringthem');

outformat1 := RECORD
         unsigned f;
             END;

outformat2 := RECORD
         string10 s;
             END;

outformat2 dst(d l) := TRANSFORM
  self.s := tostr(l.f);
end;

outformat1 dnst(d l) := TRANSFORM
  self.f := l.f;
end;

dstringed := project(d, dst(LEFT));
dnotstringed := project(d, dnst(LEFT));

if (stringthem, output(dstringed), output(dnotstringed));
