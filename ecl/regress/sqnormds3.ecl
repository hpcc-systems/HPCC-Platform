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

import AggCommon;
AggCommon.CommonDefinitions();

mask:=0x7f;

//Normalized, no filter
#if (mask & 0x01 != 0)
output(local(table(sqNamesTable1.books, { name, author, rating100 })));
#end

//Normalized, filter on inner level
#if (mask & 0x04 != 0)
output(local(table(sqNamesTable2.books(rating100>50), { name, author, rating100 })));
#end

//Normalized, filter on outer level
#if (mask & 0x08 != 0)
output(local(table(sqNamesTable3.books(sqNamesTable3.surname='Hawthorn'), { name, author, rating100 })));
#end

//Normalized, filter on both levels
#if (mask & 0x10 != 0)
output(local(table(sqNamesTable4.books(rating100>50, sqNamesTable4.surname='Hawthorn'), { name, author, rating100 })));
#end

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
#if (mask & 0x20 != 0)
output(local(table(sqNamesTable5(surname='Hawthorn').books(rating100>50), { name, author, rating100 })));
#end

//No filter or project - need to make sure we create correctly
#if (mask & 0x40 != 0)
output(local(sqNamesTable6.books));
#end

#if (mask & 0x02 != 0)
//Multiple levels, multiple filters...
output(local(table(sqHousePersonBookDs(id != 0).persons(id != 0).books(id != 0, sqHousePersonBookDs.id != 999), { name, author, rating100 })));
#end

#option ('targetClusterType', 'roxie');
