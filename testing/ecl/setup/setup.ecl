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

import Std.File AS FileServices;

DG_OutRec norm1(DG_OutRec l, integer c) := transform
  self.DG_firstname := DG_Fnames[c];
  self := l;
  end;
DG_Norm1Recs := normalize( DG_BlankSet, 4, norm1(left, counter));

DG_OutRec norm2(DG_OutRec l, integer c) := transform
  self.DG_lastname := DG_Lnames[c];
  self := l;
  end;
DG_Norm2Recs := normalize( DG_Norm1Recs, 4, norm2(left, counter));

DG_OutRec norm3(DG_OutRec l, integer c) := transform
  self.DG_Prange := DG_Pranges[c];
  self := l;
  end;
DG_Norm3Recs := normalize( DG_Norm2Recs, 4, norm3(left, counter));

//output data files
DG_OutputRecs := DG_Norm3Recs;

//***************************************************************************

DG_OutputRecs SeqParent(DG_OutputRecs l, integer c) := transform
  self.DG_ParentID := c-1;  //use -1 so max records (16^8) fit in unsigned4
  self := l;
  end;
DG_ParentRecs := project( DG_OutputRecs, SeqParent(left, counter));

#if(DG_GenChild = TRUE)
DG_OutRecChild GenChildren(DG_OutputRecs l) := transform
  self.DG_ChildID := 0;
  self := l;
  end;
DG_ChildRecs1 := normalize( DG_ParentRecs, DG_MaxChildren, GenChildren(left));

DG_OutRecChild SeqChildren(DG_OutRecChild l, integer c) := transform
  self.DG_ChildID := c-1;
  self := l;
  end;
DG_ChildRecs := project( DG_ChildRecs1, SeqChildren(left, counter));
output(DG_ParentRecs,,DG_ParentFileOut,overwrite);
output(DG_ChildRecs,,DG_ChildFileOut,overwrite);
fileServices.AddFileRelationship( DG_ParentFileOut, DG_ChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
  #if(DG_GenGrandChild = TRUE)
DG_OutRecChild GenGrandChildren(DG_OutRecChild l) := transform
  self := l;
  end;
DG_GrandChildRecs := normalize( DG_ChildRecs, DG_MaxGrandChildren, GenGrandChildren(left));
output(DG_GrandChildRecs,,DG_GrandChildFileOut,overwrite);
fileServices.AddFileRelationship( DG_ChildFileOut, DG_GrandChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
  #end
#end
//output data files

//***************************************************************************


#if(DG_GenCSV = TRUE)
output(DG_ParentRecs,,DG_FileOut+'CSV',CSV,overwrite);
#if(DG_GenChild = TRUE)
fileServices.AddFileRelationship( DG_ParentFileOut, DG_FileOut+'CSV', '', '', 'view', '1:1', false);
#end
#end
#if(DG_GenXML = TRUE)
output(DG_ParentRecs,,DG_FileOut+'XML',XML,overwrite);
#if(DG_GenChild = TRUE)
fileServices.AddFileRelationship( DG_ParentFileOut, DG_FileOut+'XML', '', '', 'view', '1:1', false);
#end
#end
#if(DG_GenIndex = TRUE)
EvensFilter := DG_ParentRecs.DG_firstname in [DG_Fnames[2],DG_Fnames[4],DG_Fnames[6],DG_Fnames[8],
                                              DG_Fnames[10],DG_Fnames[12],DG_Fnames[14],DG_Fnames[16]];

SEQUENTIAL( 
    PARALLEL(output(DG_ParentRecs,,DG_FileOut+'FLAT',overwrite),
             output(DG_ParentRecs(EvensFilter),,DG_FileOut+'FLAT_EVENS',overwrite)),
    PARALLEL(buildindex(DG_IndexFile,overwrite
#if (useLocal=true)
                        ,NOROOT
#end
                       ),
             buildindex(DG_IndexFileEvens,overwrite
#if (useLocal=true)
                        ,NOROOT
#end
             ))
    );

    fileServices.AddFileRelationship( __nameof__(DG_FlatFile), __nameof__(DG_IndexFile), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFile), __nameof__(DG_IndexFile), '__fileposition__', 'filepos', 'link', '1:1', true);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFileEvens), __nameof__(DG_IndexFileEvens), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFileEvens), __nameof__(DG_IndexFileEvens), '__fileposition__', 'filepos', 'link', '1:1', true);
#else
  #if(DG_GenFlat = TRUE)
    output(DG_ParentRecs,,DG_FileOut+'FLAT',overwrite);
    output(DG_ParentRecs(EvensFilter),,DG_FileOut+'FLAT_EVENS',overwrite);
  #end
#end

//Output variable length records
#if(DG_GenVar = TRUE)
DG_VarOutRec Proj1(DG_OutRec L) := TRANSFORM
  SELF := L;
  SELF.ExtraField := IF(self.DG_Prange<=10,
                        trim(self.DG_lastname[1..self.DG_Prange]+self.DG_firstname[1..self.DG_Prange],all),
                        trim(self.DG_lastname[1..self.DG_Prange-10]+self.DG_firstname[1..self.DG_Prange-10],all));
END;
DG_VarOutRecs := PROJECT(DG_ParentRecs,Proj1(LEFT));

sequential(
  output(DG_VarOutRecs,,DG_FileOut+'VAR',overwrite),
  buildindex(DG_VarIndex, overwrite
#if (useLocal=true)
  ,NOROOT
#end
  ),
  buildindex(DG_VarVarIndex, overwrite
#if (useLocal=true)
  ,NOROOT
#end
  ),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarIndex), '__fileposition__', '__filepos', 'link', '1:1', true),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarVarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarVarIndex), '__fileposition__', '__filepos', 'link', '1:1', true)
);
#end

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
    ], sqHousePersonBookRec);


//First reproject the datasets to 

sqBookIdRec addIdToBook(sqBookRec l) := 
            transform
                self.id := 0;
                self := l;
            end;

sqPersonBookIdRec addIdToPerson(sqPersonBookRec l) := 
            transform
                unsigned2 aage := if (l.dob < baseDate, (unsigned2)((baseDate - l.dob) / 10000), 0);
                self.id := 0;
                self.books := project(l.books, addIdToBook(LEFT));
                self.aage := if (aage > 200, 99, aage);
                self := l;
            end;

sqHousePersonBookIdRec addIdToHouse(sqHousePersonBookRec l) := 
            transform
                self.id := 0;
                self.persons := project(l.persons, addIdToPerson(LEFT));
                self := l;
            end;


projected := project(rawHouse, addIdToHouse(LEFT));

//version 1 assign unique ids a really inefficient way...
//doesn't actually work....

sqBookIdRec setBookId(sqHousePersonBookIdRec lh, sqBookIdRec l, unsigned4 basebookid) := 
            transform
                unsigned maxbookid := max(lh.persons, max(lh.persons.books, id));
                self.id := if(maxbookid=0, basebookid, maxbookid)+1;
                self := l;
            end;

sqPersonBookIdRec setPersonId(sqHousePersonBookIdRec lh, sqPersonBookIdRec l, unsigned4 basepersonid, unsigned4 basebookid) := 
            transform
                unsigned4 maxpersonid := max(lh.persons, id);
                self.id := if(maxpersonid=0, basepersonid, maxpersonid)+1;
                self.books := project(l.books, setBookId(lh, LEFT, basebookid));
                self := l;
            end;

sqHousePersonBookIdRec setHouseId(sqHousePersonBookIdRec l, sqHousePersonBookIdRec r, unsigned4 id) := 
            transform
                unsigned prevmaxpersonid := max(l.persons, id);
                unsigned prevmaxbookid := max(l.persons, max(l.persons.books, id));
                self.id := id;
                self.persons := project(r.persons, setPersonId(r, LEFT, prevmaxpersonid, prevmaxbookid));
                self := r;
            end;


final1 := iterate(projected, setHouseId(LEFT, RIGHT, counter));


//------------------ Common extraction functions... ---------------

sqHouseIdRec extractHouse(sqHousePersonBookIdRec l) :=
            TRANSFORM
                SELF := l;
            END;

sqPersonBookRelatedIdRec extractPersonBook(sqHousePersonBookIdRec l, sqPersonBookIdRec r) :=
            TRANSFORM
                SELF.houseid := l.id;
                SELF := r;
            END;

sqPersonRelatedIdRec extractPerson(sqPersonBookRelatedIdRec l) :=
            TRANSFORM
                SELF := l;
            END;

sqBookRelatedIdRec extractBook(sqBookIdRec l, unsigned4 personid) :=
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

sqPersonBookRelatedIdRec expandPerson(sqPersonRelatedIdRec l) :=
        TRANSFORM
            SELF := l;
            SELF.books := [];
        END;

sqHousePersonBookIdRec expandHouse(sqHouseIdRec l) :=
        TRANSFORM
            SELF := l;
            SELF.persons := [];
        END;

sqPersonBookRelatedIdRec combinePersonBook(sqPersonBookRelatedIdRec l, sqBookRelatedIdRec r) :=
        TRANSFORM
            SELF.books := l.books + row({r.id, r.name, r.author, r.rating100, r.price}, sqBookIdRec);
            SELF := l;
        END;

sqHousePersonBookIdRec combineHousePerson(sqHousePersonBookIdRec l, sqPersonBookRelatedIdRec r) :=
        TRANSFORM
            SELF.persons := l.persons + row(r, sqPersonBookIdRec);
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

simplePersonBooks := project(personBooks, transform(sqSimplePersonBookRec, SELF := LEFT, SELF.limit.booklimit := LEFT.booklimit));

output(final,, sqHousePersonBookName,overwrite);
output(personBooks,, sqPersonBookName,overwrite);
output(houseOut,,sqHouseName,overwrite);
output(personOut,,sqPersonName,overwrite);
output(bookOut,,sqBookName,overwrite);

output(simplePersonBooks,, sqSimplePersonBookName,overwrite);
buildindex(
#if (useLocal=true)
  DISTRIBUTE(sqSimplePersonBookDs, IF(surname > 'G', 0, 1)),
#else
  sqSimplePersonBookDs, 
#end
  { surname, forename, aage  }, { sqSimplePersonBookDs }, sqSimplePersonBookIndexName, overwrite
#if (useLocal=true)
 , NOROOT
#end
);
fileServices.AddFileRelationship( __nameof__(sqSimplePersonBookDs), sqSimplePersonBookIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sqSimplePersonBookDs), sqSimplePersonBookIndexName, '__fileposition__', 'filepos', 'link', '1:1', true);

fileServices.AddFileRelationship( sqHouseName, sqPersonName, 'id', 'houseid', 'link', '1:M', false);
fileServices.AddFileRelationship( sqPersonName, sqBookName, 'id', 'personid', 'link', '1:M', false);

fileServices.AddFileRelationship( sqHouseName, sqHousePersonBookName, 'id', 'id', 'link', '1:1', false);
fileServices.AddFileRelationship( sqHouseName, sqPersonBookName, 'id', 'houseid', 'link', '1:M', false);

//Now build some indexes - with numeric fields in the key
buildindex(sqHouseExDs, { id }, { addr, filepos }, sqHouseIndexName+'ID', overwrite);
buildindex(sqPersonExDs, { id }, { filepos }, sqPersonIndexName+'ID', overwrite);
buildindex(sqBookExDs, { id }, { filepos }, sqBookIndexName+'ID', overwrite);

fileServices.AddFileRelationship( __nameof__(sqHouseExDs), sqHouseIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sqHouseExDs), sqHouseIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sqPersonExDs), sqPersonIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sqPersonExDs), sqPersonIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sqBookExDs), sqBookIndexName+'ID', '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sqBookExDs), sqBookIndexName+'ID', '__fileposition__', 'filepos', 'link', '1:1', true);

//Some more conventional indexes - some requiring a double lookup to resolve the payload
buildindex(sqHouseExDs, { string40 addr := sqHouseExDs.addr, postcode }, { filepos }, sqHouseIndexName, overwrite);
buildindex(sqPersonExDs, { string40 forename := sqPersonExDs.forename, string40 surname := sqPersonExDs.surname }, { id }, sqPersonIndexName, overwrite);
buildindex(sqBookExDs, { string40 name := sqBookExDs.name, string40 author := sqBookExDs.author }, { id }, sqBookIndexName, overwrite);

fileServices.AddFileRelationship( __nameof__(sqHouseExDs), sqHouseIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( __nameof__(sqHouseExDs), sqHouseIndexName, '__fileposition__', 'filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sqPersonExDs), sqPersonIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( sqPersonIndexName+'ID', sqPersonIndexName, 'id', 'id', 'link', '1:1', true);
fileServices.AddFileRelationship( __nameof__(sqBookExDs), sqBookIndexName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( sqBookIndexName+'ID', sqBookIndexName, 'id', 'id', 'link', '1:1', true);

//Should try creating a dataset with a set of ids which are used as a link...  (e.g., bookids->bookfile)

DG_MemFileRec t_u2(DG_MemFileRec l, integer c) := transform self.u2 := c-2; self := l; END;
DG_MemFileRec t_u3(DG_MemFileRec l, integer c) := transform self.u3 := c-2; self := l; END;
DG_MemFileRec t_bu2(DG_MemFileRec l, integer c) := transform self.bu2 := c-2; self := l; END;
DG_MemFileRec t_bu3(DG_MemFileRec l, integer c) := transform self.bu3 := c-2; self := l; END;
DG_MemFileRec t_i2(DG_MemFileRec l, integer c) := transform self.i2 := c-2; self := l; END;
DG_MemFileRec t_i3(DG_MemFileRec l, integer c) := transform self.i3 := c-2; self := l; END;
DG_MemFileRec t_bi2(DG_MemFileRec l, integer c) := transform self.bi2 := c-2; self := l; END;
DG_MemFileRec t_bi3(DG_MemFileRec l, integer c) := transform self.bi3 := c-2; self := l; END;

n_blank := dataset([{0,0,0,0, 0,0,0,0}],DG_MemFileRec);

n_u2 := NORMALIZE(n_blank, 4, t_u2(left, counter));
n_u3 := NORMALIZE(n_u2, 4, t_u3(left, counter));

n_bu2 := NORMALIZE(n_u3, 4, t_bu2(left, counter));
n_bu3 := NORMALIZE(n_bu2, 4, t_bu3(left, counter));

n_i2 := NORMALIZE(n_bu3, 4, t_i2(left, counter));
n_i3 := NORMALIZE(n_i2, 4, t_i3(left, counter));

n_bi2 := NORMALIZE(n_i3, 4, t_bi2(left, counter));
n_bi3 := NORMALIZE(n_bi2, 4, t_bi3(left, counter));

output(n_bi3,,DG_MemFileName,overwrite);


DG_IntegerRecord createIntegerRecord(unsigned8 c) := transform
    SELF.i6 := c;
    SELF.nested.i4 := c;
    SELF.nested.u3 := c;
    SELF.i5 := c;
    SELF.i3 := c;
END;

singleNullRowDs := dataset([transform({unsigned1 i}, self.i := 0;)]);
output(normalize(singleNullRowDs, 100, createIntegerRecord(counter)),,DG_IntegerDatasetName,overwrite);
build(DG_IntegerIndex,overwrite);
