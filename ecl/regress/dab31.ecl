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

I := record
 unsigned integer1 presflag2;
 ifblock(self.presflag2 & 0x08 != 0)
    decimal3 mem_num1;
 end;

end;

tempdataset := dataset('ecl_test::fb38_sample', I, flat);
O := record
 unsigned integer1 presflag2;

 decimal3 mem_num1;
end;

O trans(i l) := transform
  self := l;
  end;

p := project(tempdataset,trans(left));

output(p)