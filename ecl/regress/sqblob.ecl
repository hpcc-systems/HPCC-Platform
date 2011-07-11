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

#option ('targetClusterType', 'roxie');

import * from sq;
DeclareCommon();

//UseStandardFiles

o1 := output(table(sqSimplePersonBookIndex, { dataset books := sqSimplePersonBookIndex.books, sqSimplePersonBookIndex.surname, count(group) }, sqSimplePersonBookIndex.surname[2..], few));

o4 := output(table(sqSimplePersonBookIndex, { dataset books := sqSimplePersonBookIndex.books, sqSimplePersonBookIndex.surname, count(group) }, sqSimplePersonBookIndex.surname, few));

o2 := output(table(sqPersonBookExDs, { dataset books := sqPersonBookExDs.books, sqPersonBookExDs.surname, sqPersonBookExDs.filepos, count(group) }, sqPersonBookExDs.surname, few));

o3 := output(table(sqSimplePersonBookIndex.books, { id }));

sequential(o1, o2, o3, o4);