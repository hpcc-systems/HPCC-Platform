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

//Simple disk aggregate
output(table(sqNamesIndex1, { surname, sum(group, aage) }, surname, few));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesIndex2(surname != 'Halliday'), { max(group, aage), surname }, surname, few));

//Special case count.
output(table(sqNamesIndex3(forename = 'Gavin'), { count(group), surname }, surname, few));

//Sub query needs serializing or repeating....

// A simple inline subquery
output(table(sqNamesIndex4, { count(books), sumage := sum(group, aage) }, count(books), few));

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesIndex5.books, name)[2].name;
output(table(sqNamesIndex5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few));

//Access to file position - ensure it is serialized correctly!
output(table(sqNamesIndex6, { filepos DIV 8192, sumage := sum(group, aage) }, filepos DIV 8192, few));

// An out of line subquery (caused problems accessing parent inside sort criteria
output(table(sqNamesIndex7, { count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few));
