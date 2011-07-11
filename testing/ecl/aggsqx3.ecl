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

//Simple disk aggregate
output(preload(sqHousePersonBookDs), { dataset sort(table(persons, { surname, sum(group, aage) }, surname, few), surname)});

//Filtered disk aggregate, which also requires a beenProcessed flag
output(sqHousePersonBookDs, { dataset sort(table(persons(surname != 'Halliday'), { max(group, aage), surname }, surname, few), surname)});

//check literals are assigned
output(sqHousePersonBookDs, { dataset sort(table(persons(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few), surname) });

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sqHousePersonBookDs, { dataset sort(table(persons, { cnt := count(books), sumage := sum(group, aage) }, count(books), few), cnt)});

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqHousePersonBookDs.persons.books, name)[2].name;
output(sqHousePersonBookDs, { dataset sort(table(persons, { sbn := secondBookName, sumage := sum(group, aage) }, secondBookName, few), sbn)});
