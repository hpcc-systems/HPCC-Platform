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
//nothor
//nothorlcr

#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

mask:=0xffff;

//Normalized, no filter
#if (mask & 0x01 != 0)
output(sqNamesTable1.books, { name, author, rating100 });
#end

//Normalized, filter on inner level
#if (mask & 0x04 != 0)
output(sqNamesTable2.books(rating100>50), { name, author, rating100 });
#end

//Normalized, filter on outer level
#if (mask & 0x08 != 0)
output(sqNamesTable3.books(sqNamesTable3.surname='Halliday'), { name, author, rating100 });
#end

//Normalized, filter on both levels
#if (mask & 0x10 != 0)
output(sqNamesTable4.books(rating100>50, sqNamesTable4.surname='Halliday'), { name, author, rating100 });
#end

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
#if (mask & 0x20 != 0)
output(sqNamesTable5(surname='Halliday').books(rating100>50), { name, author, rating100 });
#end

//No filter or project - need to make sure we create correctly
#if (mask & 0x40 != 0)
output(sqNamesTable6.books);
#end

#if (mask & 0x02 != 0)
//Multiple levels, multiple filters...
output(sqHousePersonBookDs(id != 0).persons(id != 0).books(id != 0, sqHousePersonBookDs.id != 999), { name, author, rating100 });
#end

// And the local variants....

//Normalized, no filter
#if (mask & 0x81 = 0x81)
output(allnodes(local(sqNamesTable1.books)), { name, author, rating100 });
#end

//Normalized, filter on inner level
#if (mask & 0x84 = 0x84)
output(allnodes(local(sqNamesTable2.books(rating100>50))), { name, author, rating100 });
#end

//Normalized, filter on outer level
#if (mask & 0x88 = 0x88)
output(allnodes(local(sqNamesTable3.books(sqNamesTable3.surname='Halliday'))), { name, author, rating100 });
#end

//Normalized, filter on both levels
#if (mask & 0x90 = 0x90)
output(allnodes(local(sqNamesTable4.books(rating100>50, sqNamesTable4.surname='Halliday'))), { name, author, rating100 });
#end

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
#if (mask & 0xa0 = 0xa0)
output(allnodes(local(sqNamesTable5(surname='Halliday').books(rating100>50))), { name, author, rating100 });
#end

//No filter or project - need to make sure we create correctly
#if (mask & 0xc0 = 0xc0)
output(allnodes(local(sqNamesTable6.books)));
#end

#if (mask & 0x82 = 0x82)
//Multiple levels, multiple filters...
output(allnodes(local(sqHousePersonBookDs(id != 0).persons(id != 0).books(id != 0, sqHousePersonBookDs.id != 999))), { name, author, rating100 });
#end

