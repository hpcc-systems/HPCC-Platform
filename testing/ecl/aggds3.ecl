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
secondBookName := (string20)sort(sqNamesTable5.books, name)[2].name;

//Simple disk aggregate
output(sort(table(sqNamesTable1, { surname, sumage := sum(group, aage) }, surname, few),surname));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(sort(table(sqNamesTable2(surname != 'Halliday'), { max(group, aage), surname }, surname, few),surname));

//check literals are assigned
output(sort(table(sqNamesTable3(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few),surname));

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sort(table(sqNamesTable4, { cnt := count(books), sumage := sum(group, aage) }, count(books), few),cnt));

output(sort(table(sqNamesTable5, { sbn := secondBookName, sumage := sum(group, aage) }, secondBookName, few),sbn));

// An out of line subquery (caused problems accessing parent inside sort criteria
output(sort(table(sqNamesTable7, { cnt := count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few),cnt));

//Bizarre - add a dataset that needs serialization/deserialisation to enusre cloned correctly
output(sort(table(nofold(sqNamesTable2)(surname != 'Halliday'), { max(group, aage), surname, dataset books }, surname, few),surname));
