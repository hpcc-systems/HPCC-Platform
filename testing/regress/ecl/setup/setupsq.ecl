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

import Std.File AS FileServices;
import $.sq;

//******************************** Child query setup code ***********************

udecimal8 baseDate := 20050101;

rawHouse := dataset([
    { '99 Maltings Road', 'SW1A0AA', 1720,
        [{ 'Gavin', 'Halliday', 19700101, 1000, 0,
            [
            { 'To kill a mocking bird', 'Harper Lee', 95},
            { 'Clarion Language Manual', 'Richard Taylor', 1, 399.99 },
            { 'The bible', 'Various', 98 },
            { 'Compilers', 'Aho, Sethi, Ullman', 80 },
            { 'Lord of the Rings', 'JRR Tolkien', 95 }
            ]
        },
        { 'Abigail', 'Halliday', 20000101, 40, 0,
            [
            { 'The thinks you can think', 'Dr. Seuss', 90, 5.99 },
            { 'Where is flop?', '', 85, 4.99 },
            { 'David and Goliath', '', 20, 2.99 },
            { 'The story of Jonah', '', 80, 6.99 }
            ]
        },
        { 'Liz', 'Halliday', 19700909, 0, 0,
            [
            { 'The Life of Pi', 'Yan Martel', 90 },
            { 'BNF', 'Various', 60 },
            { 'The Third Policeman', 'Flann O\'Brien', 85 }
            ]
        }]
    },
    { 'Buckingham Palace', 'WC1', 1702,
        [{'Elizabeth', 'Windsor', 19260421, 0, 0,
            [
            { 'The bible', 'Various', 93 },
            { 'The Prince', 'Machiavelli', 40 },
            { 'Atlas of the world', 'Times', 70 },
            { 'Girl guide annual', 'Various', 50 },
            { 'Rwandan war dances', 'Unknown', 30 }
            ]
        },
        {'Philip', 'Windsor', 19210610, 0, 0,
            [
            { 'Greek tragedies', 'Various', 30 },
            { 'Social etiquette', 'RU Correct', 10},
            { 'The Rituals of Dinner: The Origins, Evolution, Eccentricities and the Meaning of Table Manners', 'Margaret Visser', 60 },
            { 'The cat in the hat', 'Dr. Seuss', 85 }
            ]
        }]
    },
    { 'Bedrock', '', 0,
        [{'Fred', 'Flintstone', 00000101, 0, 0, [] },
        {'Wilma', 'Flintstone', 00020202, 0, 0,
            [
            { 'Dinosaur stews', 'Trog', 55 }
            ]
        }]
    },
    { 'Wimpole Hall', 'AG1 6QT', 1203,
        [{'John', 'Grabdale', 19361008, 0, 0,
            [
            { 'Pride and prejudice, 1st edition', 'Jane Austen', 95, 12000 },
            { 'Mein Kampf, 1st edition', 'Adolph Hitler', 80, 10000 }
            ]
        }]
    },
    { '56 New Road', 'SG8 1S2', 2003,
        [{'Brian', 'Jones', 19540206, 0, 0,
            [
            { 'All quiet on the western front', 'Erich Maria Remarque', 85, 4.99 },
            { 'Fox in Socks', 'Dr. Seuss', 99, 4.99 }
            ]
        },
        {'Julia', 'Jones', 19550312, 0, 0,
            [
            { 'Zen and the art of motorcyle maintenance', 'Robert Pirsig', 90, 7.99 },
            { 'Where the wild things are', 'Maurice Sendak', 70, 4.79 },
            { 'The bible', 'Various', 10 , 5.99 },
            { 'The cat in the hat', 'Dr. Seuss', 80 }
            ]
        }]
    }
    ], sq.HousePersonBookRec);


//First reproject the datasets to

sq.BookIdRec addIdToBook(sq.BookRec l) :=
            transform
                self.id := 0;
                self := l;
            end;

sq.PersonBookIdRec addIdToPerson(sq.PersonBookRec l) :=
            transform
                unsigned2 aage := if (l.dob < baseDate, (unsigned2)((baseDate - l.dob) / 10000), 0);
                self.id := 0;
                self.books := project(l.books, addIdToBook(LEFT));
                self.aage := if (aage > 200, 99, aage);
                self := l;
            end;

sq.HousePersonBookIdRec addIdToHouse(sq.HousePersonBookRec l) :=
            transform
                self.id := 0;
                self.persons := project(l.persons, addIdToPerson(LEFT));
                self := l;
            end;


projected := project(rawHouse, addIdToHouse(LEFT));

//version 1 assign unique ids a really inefficient way...
//doesn't actually work....

sq.BookIdRec setBookId(sq.HousePersonBookIdRec lh, sq.BookIdRec l, unsigned4 basebookid) :=
            transform
                unsigned maxbookid := max(lh.persons, max(lh.persons.books, id));
                self.id := if(maxbookid=0, basebookid, maxbookid)+1;
                self := l;
            end;

sq.PersonBookIdRec setPersonId(sq.HousePersonBookIdRec lh, sq.PersonBookIdRec l, unsigned4 basepersonid, unsigned4 basebookid) :=
            transform
                unsigned4 maxpersonid := max(lh.persons, id);
                self.id := if(maxpersonid=0, basepersonid, maxpersonid)+1;
                self.books := project(l.books, setBookId(lh, LEFT, basebookid));
                self := l;
            end;

sq.HousePersonBookIdRec setHouseId(sq.HousePersonBookIdRec l, sq.HousePersonBookIdRec r, unsigned4 id) :=
            transform
                unsigned prevmaxpersonid := max(l.persons, id);
                unsigned prevmaxbookid := max(l.persons, max(l.persons.books, id));
                self.id := id;
                self.persons := project(r.persons, setPersonId(r, LEFT, prevmaxpersonid, prevmaxbookid));
                self := r;
            end;


final1 := iterate(projected, setHouseId(LEFT, RIGHT, counter));


//------------------ Common extraction functions... ---------------

sq.HouseIdRec extractHouse(sq.HousePersonBookIdRec l) :=
            TRANSFORM
                SELF := l;
            END;

sq.PersonBookRelatedIdRec extractPersonBook(sq.HousePersonBookIdRec l, sq.PersonBookIdRec r) :=
            TRANSFORM
                SELF.houseid := l.id;
                SELF := r;
            END;

sq.PersonRelatedIdRec extractPerson(sq.PersonBookRelatedIdRec l) :=
            TRANSFORM
                SELF := l;
            END;

sq.BookRelatedIdRec extractBook(sq.BookIdRec l, unsigned4 personid) :=
            TRANSFORM
                SELF.personid := personid;
                SELF := l;
            END;



//------------------- Add Sequence numbers by normalized/project/denormalize

//normalize, adding parent ids as we do it.  Once all normalized and sequenced then combine them back together

DoAssignSeq(ds, o) := macro
#uniquename (trans)
typeof(ds) %trans%(ds l, unsigned c) :=
        transform
            self.id := c;
            self := l;
        end;
o := sorted(project(ds, %trans%(LEFT, COUNTER)), id);
endmacro;

DoAssignSeq(projected, projectedSeq);
normSeqHouse := project(projectedSeq, extractHouse(LEFT));
normPersonBooks := normalize(projectedSeq, left.persons, extractPersonBook(LEFT, RIGHT));
DoAssignSeq(normPersonBooks, normSeqPersonBooks);
normSeqPerson := project(normSeqPersonBooks, extractPerson(LEFT));
normBook := normalize(normSeqPersonBooks, count(left.books), extractBook(LEFT.books[COUNTER], LEFT.id));
DoAssignSeq(normBook, normSeqBook);

// finally denormalize by joining back together.

sq.PersonBookRelatedIdRec expandPerson(sq.PersonRelatedIdRec l) :=
        TRANSFORM
            SELF := l;
            SELF.books := [];
        END;

sq.HousePersonBookIdRec expandHouse(sq.HouseIdRec l) :=
        TRANSFORM
            SELF := l;
            SELF.persons := [];
        END;

sq.PersonBookRelatedIdRec combinePersonBook(sq.PersonBookRelatedIdRec l, sq.BookRelatedIdRec r) :=
        TRANSFORM
            SELF.books := l.books + row({r.id, r.name, r.author, r.rating100, r.price}, sq.BookIdRec);
            SELF := l;
        END;

sq.HousePersonBookIdRec combineHousePerson(sq.HousePersonBookIdRec l, sq.PersonBookRelatedIdRec r) :=
        TRANSFORM
            SELF.persons := l.persons + row(r, sq.PersonBookIdRec);
            SELF := l;
        END;

normSeqHouseEx := project(normSeqHouse, expandHouse(LEFT));
normSeqPersonEx := project(normSeqPerson, expandPerson(LEFT));
normSeqPersonBook := denormalize(normSeqPersonEx, sorted(normSeqBook, personid), left.id = right.personid, combinePersonBook(left, right), local);
final3 := denormalize(normSeqHouseEx, sorted(normSeqPersonBook, houseid), left.id = right.houseid, combineHousePerson(left, right), local);



//------------ Now generate the different output files.... -----------------
// Try and do everything as many different ways as possible...!

final := final3;

houseOut := project(final, extractHouse(LEFT));
personBooks := normalize(final, left.persons, extractPersonBook(LEFT, RIGHT));
personOut := project(personBooks, extractPerson(LEFT));
bookOut := normalize(personBooks, count(left.books), extractBook(LEFT.books[COUNTER], LEFT.id));

simplePersonBooks := project(personBooks, transform(sq.SimplePersonBookRec, SELF := LEFT, SELF.limit.booklimit := LEFT.booklimit));

output(final,, sq.HousePersonBookName,overwrite);
output(personBooks,, sq.PersonBookName,overwrite);
output(houseOut,,sq.HouseName,overwrite);
output(personOut,,sq.PersonName,overwrite);
output(bookOut,,sq.BookName,overwrite);

output(simplePersonBooks,, sq.SimplePersonBookName,overwrite);
buildindex(
  sq.SimplePersonBookDs,
  { surname, forename, aage  }, { sq.SimplePersonBookDs }, sq.SimplePersonBookIndexName, overwrite
);
fileServices.AddFileRelationship( __nameof__(sq.SimplePersonBookDs), sq.SimplePersonBookIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sq.SimplePersonBookDs), sq.SimplePersonBookIndexName, '__fileposition__', 'filepos', 'link', '1:1', true);

fileServices.AddFileRelationship( sq.HouseName, sq.PersonName, 'id', 'houseid', 'link', '1:M', false);
fileServices.AddFileRelationship( sq.PersonName, sq.BookName, 'id', 'personid', 'link', '1:M', false);

fileServices.AddFileRelationship( sq.HouseName, sq.HousePersonBookName, 'id', 'id', 'link', '1:1', false);
fileServices.AddFileRelationship( sq.HouseName, sq.PersonBookName, 'id', 'houseid', 'link', '1:M', false);

//Now build some indexes - with numeric fields in the key
buildindex(sq.HouseExDs, { id }, { addr, filepos }, sq.HouseIndexName+'ID', overwrite);
buildindex(sq.PersonExDs, { id }, { filepos }, sq.PersonIndexName+'ID', overwrite);
buildindex(sq.BookExDs, { id }, { filepos }, sq.BookIndexName+'ID', overwrite);

fileServices.AddFileRelationship( __nameof__(sq.HouseExDs), sq.HouseIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sq.HouseExDs), sq.HouseIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sq.PersonExDs), sq.PersonIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sq.PersonExDs), sq.PersonIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sq.BookExDs), sq.BookIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sq.BookExDs), sq.BookIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);

//Some more conventional indexes - some requiring a double lookup to resolve the payload
buildindex(sq.HouseExDs, { string40 addr := sq.HouseExDs.addr, postcode }, { filepos }, sq.HouseIndexName, overwrite);
buildindex(sq.PersonExDs, { string40 forename := sq.PersonExDs.forename, string40 surname := sq.PersonExDs.surname }, { id }, sq.PersonIndexName, overwrite);
buildindex(sq.BookExDs, { string40 name := sq.BookExDs.name, string40 author := sq.BookExDs.author }, { id }, sq.BookIndexName, overwrite);

fileServices.AddFileRelationship( __nameof__(sq.HouseExDs), sq.HouseIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sq.HouseExDs), sq.HouseIndexName, '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sq.PersonExDs), sq.PersonIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( sq.PersonIndexName+'ID', sq.PersonIndexName, 'id', 'id', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sq.BookExDs), sq.BookIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( sq.BookIndexName+'ID', sq.BookIndexName, 'id', 'id', 'link', '1:1', true);
