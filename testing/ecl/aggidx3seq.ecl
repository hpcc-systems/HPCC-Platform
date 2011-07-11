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
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesIndex5.books, name)[2].name;

sequential(
//Simple disk aggregate
output(sort(table(sqNamesIndex1, { string surname := trim(sqNamesIndex1.surname), sum(group, aage) }, trim(surname), few), surname)),


//Filtered disk aggregate, which also requires a beenProcessed flag
output(sort(table(sqNamesIndex2(surname != 'Halliday'), { max(group, aage), surname }, surname, few), surname)),

//Special case count.
output(sort(table(sqNamesIndex3(forename = 'Gavin'), { count(group), surname }, surname, few), surname)),

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sort(table(sqNamesIndex4, { cntBooks:= count(books), sumage := sum(group, aage) }, count(books), few), cntBooks, sumage)),

output(sort(table(sqNamesIndex5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few), secondbookname)),

//Access to file position - ensure it is serialized correctly!
output(sort(table(sqNamesIndex6, { pos := filepos DIV 8192, sumage := sum(group, aage) }, filepos DIV 8192, few), pos, sumage)),

// An out of line subquery (caused problems accessing parent inside sort criteria
output(sort(table(sqNamesIndex7, { numBooks := count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few), numbooks, sumage))
);
