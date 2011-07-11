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

#option ('globalFold', false);
#option ('foldConstantCast', false);

output(intformat(1234, 8, 0));
output(intformat(1234, 8, 1));
output(intformat(1234, 4, 0));
output(intformat(1234, 4, 1));
output(intformat(1234, 3, 0));
output(intformat(1234, 3, 1));

output((integer1)(0x123456789abcdef0+0));
output((integer2)(0x123456789abcdef0+0));
output((integer3)(0x123456789abcdef0+0));
output((integer4)(0x123456789abcdef0+0));
output((integer5)(0x123456789abcdef0+0));
output((integer6)(0x123456789abcdef0+0));
output((integer7)(0x123456789abcdef0+0));
output((integer8)(0x123456789abcdef0+0));

output((unsigned1)(0x123456789abcdef0+0));
output((unsigned2)(0x123456789abcdef0+0));
output((unsigned3)(0x123456789abcdef0+0));
output((unsigned4)(0x123456789abcdef0+0));
output((unsigned5)(0x123456789abcdef0+0));
output((unsigned6)(0x123456789abcdef0+0));
output((unsigned7)(0x123456789abcdef0+0));
output((unsigned8)(0x123456789abcdef0+0));
