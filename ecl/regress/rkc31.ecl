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

outformat := RECORD
               IFBLOCK(stringthem=true)
         unsigned f;
               END;
               IFBLOCK(stringthem=false)
         string10 s;
               END
             END;

outformat dst(d l) := TRANSFORM
  self.f := 44;
  self.s := tostr(l.f);
end;

outformat dnst(d l) := TRANSFORM
  self.f := l.f;
  self.s := 'huh';
end;

dstringed := project(d, dst(LEFT));
dnotstringed := project(d, dnst(LEFT));

output(if (stringthem, dstringed, dnotstringed));
