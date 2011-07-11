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

//UseStandardFiles

fail('This test is temporarily disabled because it causes lockups in roxie/hthor and probably thor');
/*
output(choosen(DG_IntegerIndex, 3));

output(DG_IntegerIndex(keyed(i6 = 4)));

//Filters on nested integer fields work, but range filters will not because the fields
//are not biased.
output(DG_IntegerIndex(wild(i6),keyed(nested.i4 = 5)));
output(DG_IntegerIndex(wild(i6),wild(nested.i4),keyed(nested.u3 = 6)));
output(DG_IntegerIndex(i5 = 7));
output(DG_IntegerIndex(i3 = 8));
*/