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

rec1 := record
                string1 c;
                string2 d;
end;

rec2 := record
                string1 a;
                rec1 b;
end;


r0 := row({'2', '34'}, rec1);

r1 := row({'1', {'2', '34'}}, rec2);

r2 := row({'1', r0}, rec2);

output(dataset(r2));
