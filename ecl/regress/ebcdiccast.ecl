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






integer8 i := 12345 : stored('i');
integer8 j := 12345 : stored('j');

rec := record
integer x;
ebcdic string s;
    end;

ds := dataset('ds', rec, thor);
ds2 := table(ds,
            {
            ebcdic string s2 := if(x < 10, (ebcdic string)i, (ebcdic string)j);
            data s3 := if(x < 10, (data)i, (data)j);
            });
output(ds2);
