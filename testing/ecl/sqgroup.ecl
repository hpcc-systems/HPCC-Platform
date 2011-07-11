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

// Grouped operations on child datasets.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

house := sqHousePersonBookDs;
persons := sqHousePersonBookDs.persons;
books := persons.books;

booksDs := sqBookDs(personid = persons.id);
personsDs := sqPersonDs(houseid = sqHousePersonBookDs.id);
booksDsDs := sqBookDs(personid = personsDs.id);
personsDsDs := sqPersonDs(houseid = sqHouseDs.id);
booksDsDsDs := sqBookDs(personid = personsDsDs.id);


// Count how many books each surname has - daft way#1: use rollup!
personSummaryRec := { persons.surname, cnt := count(books) };
personSummary := table(persons, personSummaryRec);
grPersonSummary := group(personSummary, surname, all);
personSummaryRec tr1(personSummaryRec l, personSummaryRec r) := 
        transform
            self.cnt := l.cnt + r.cnt;
            self := l;
        end;
ruPersonSummary := rollup(grPersonSummary, tr1(LEFT, RIGHT), true);
output(house, { addr, dataset(personSummaryRec) summary := group(ruPersonSummary); });

// Daft way #2a - using aggregate
tgPersonSummary := table(grPersonSummary, { integer cnt := sum(group, grPersonSummary.cnt), surname });

personSummaryRec tr2a(tgPersonSummary l) := 
        transform
            self := l;
        end;
tpgPersonSummary := project(tgPersonSummary, tr2a(LEFT));

output(house, { addr, dataset(personSummaryRec) summary := tpgPersonSummary; });

// Daft way #2b - using aggregate - need to support sum(group) etc inside transform parameters
personSummaryRec tr2b(personSummaryRec l) := 
        transform
            self.cnt := sum(group, l.cnt);
            self := l;
        end;
agPersonSummary := project(grPersonSummary, tr2b(LEFT));

//output(house, { addr, dataset(personSummaryRec) summary := agPersonSummary; });

// Daft way #3 - using iterate and dedup!
personSummaryRec tr3(personSummaryRec l, personSummaryRec r) := 
        transform
            self.cnt := l.cnt + r.cnt;
            self := r;
        end;
gitPersonSummary := iterate(grPersonSummary, tr3(LEFT, RIGHT));
gdpPersonSummary := dedup(gitPersonSummary, right);

output(house, { addr, dataset(personSummaryRec) summary := group(gdpPersonSummary); });

// Finally a Daft way #4 - using non-grouped iterate and dedup!
personSummaryRec tr4(personSummaryRec l, personSummaryRec r) := 
        transform
            self.cnt := if (l.surname=r.surname, l.cnt + r.cnt, r.cnt);
            self := r;
        end;
itPersonSummary := iterate(sort(personSummary, surname), tr4(LEFT, RIGHT));
dpPersonSummary := dedup(itPersonSummary, surname, right);

output(house, { addr, dataset(personSummaryRec) summary := dpPersonSummary; });

