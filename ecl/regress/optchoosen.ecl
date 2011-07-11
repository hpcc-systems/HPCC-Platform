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

rt_fixed1_ebcdic := 
  record
string1 col1;
  end;
d1 := DATASET('~file::127.0.0.1::temp::people', rt_fixed1_ebcdic, 
Flat);
ebcd := ebcdic(d1);
ascd := ascii(ebcd);

OUTPUT(choosen(d1,100));
OUTPUT(choosen(ebcd,100));
OUTPUT(choosen(ascd,100));

