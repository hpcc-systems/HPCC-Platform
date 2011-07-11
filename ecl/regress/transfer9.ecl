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

foo := record
  string20 line;
end;
dsfoo := nofold(dataset([{'12345678901234567890'}],foo));
bar := record
     string5 a;
     string5 b;
     string5 c;
     string5 d;
end;

bar tfoo(foo l) := transform
  self := transfer(l,typeof(bar));
end;
dsbar := project(dsfoo,tfoo(left));

output(dsbar);

