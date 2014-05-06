/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

import $.setup;
sq := setup.sq('hthor');

// Grouped operations on child datasets.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

house := sq.HousePersonBookDs;
persons := sq.HousePersonBookDs.persons;
books := persons.books;

booksDs := sq.BookDs(personid = persons.id);
personsDs := sq.PersonDs(houseid = sq.HousePersonBookDs.id);
booksDsDs := sq.BookDs(personid = personsDs.id);
personsDsDs := sq.PersonDs(houseid = sq.HouseDs.id);
booksDsDsDs := sq.BookDs(personid = personsDsDs.id);


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
