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

r1 := record
    string      s1{maxlength(30)};
    string      s2{maxlength(30)};
    string      s3{maxlength(30)};
end;

output(dataset('ds1', r1, csv),named('r1'));

r2 := record,maxsize(30+12)
    string      s1{maxlength(30)};
    string      s2{maxlength(30)};
    string      s3{maxlength(30)};
end;

output(dataset('ds2', r2, csv),named('r2'));
