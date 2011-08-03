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
output(table(sqNamesTable1, { surname, sum(group, aage) }, surname, few, keyed));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesTable2(surname != 'Hawthorn'), { max(group, aage), surname }, surname, few, keyed));

//check literals are assigned
output(table(sqNamesTable3(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few, keyed));

//Sub query needs serializing or repeating....

// A simple inline subquery
output(table(sqNamesTable4, { count(books), sumage := sum(group, aage) }, count(books), few, keyed));

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesTable5.books, name)[2].name;
output(table(sqNamesTable5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few, keyed));




// An out of line subquery (caused problems accessing parent inside sort criteria
output(table(sqNamesTable7, { count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few, keyed));

