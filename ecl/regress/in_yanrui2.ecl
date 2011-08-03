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
varstring vs1 := (varstring)('ab'+'cde');
varstring vs2 := (varstring)('cde' + 'ef');

// Will be "No" because 'abcd' in the set will be promoted to varstring, while 'abcd' outside is
// String, so the types don't match.
s := if('abcd' in [vs1, 'abcd'], 'Yes', 'No');
s;
