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
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

//Normalized, no filter
output(sqNamesIndex1.books, { name, author, rating100 });

//Normalized, filter on inner level
output(sqNamesIndex2.books(rating100>50), { name, author, rating100 });

//Normalized, filter on outer level
output(sqNamesIndex3.books(sqNamesIndex3.surname='Halliday',sqNamesIndex3.dob*2!=0), { name, author, rating100 });

//Normalized, filter on both levels
output(sqNamesIndex4.books(rating100>50, sqNamesIndex4.surname='Halliday'), { name, author, rating100 });

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
output(sqNamesIndex5(surname='Halliday',dob*2!=0).books(rating100>50), { name, author, rating100 });

//No filter or project - need to make sure we create correctly
output(sqNamesIndex6.books);
