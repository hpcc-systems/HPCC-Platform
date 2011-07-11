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

#option('foldConstantDatasets', 0);
#option('pickBestEngine', 0);
#option('layoutTranslationEnabled', 0);
#option ('useLinkedNormalize', true);
#option ('supportsLinkedChildRows', true);
#option ('implicitLinkedChildRows', true);

import std.system.thorlib,lib_stringlib;
prefix := 'hthor';
useLayoutTrans := false;
useLocal := false;
usePayload := false;
useVarIndex := false;
useDynamic := false;
setupTextFileLocation := '.::c$::edata::testing::ecl::files';
//define constants
DG_GenFlat           := true;   //TRUE gens FlatFile
DG_GenChild          := true;   //TRUE gens ChildFile
DG_GenGrandChild     := true;   //TRUE gens GrandChildFile
DG_GenIndex          := true;   //TRUE gens FlatFile AND the index
DG_GenCSV            := true;   //TRUE gens CSVFile
DG_GenXML            := true;   //TRUE gens XMLFile
DG_GenVar            := true;   //TRUE gens VarFile only IF MaxField >= 3

DG_MaxField          := 3;    // maximum number of fields to use building the data records
DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs

                    // generates (#parents * DG_MaxChildren) records
DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records

#if (useDynamic=true)
 VarString EmptyString := '' : STORED('dummy');
 filePrefix := prefix + EmptyString;
 #option ('allowVariableRoxieFilenames', 1);
#else
 filePrefix := prefix;
#end

//----------------------------- Child query related definitions ----------------------------------

// Raw record definitions:

sqHouseRec := 
            record
string          addr;
string10        postcode;
unsigned2       yearBuilt := 0;
            end;


sqPersonRec := 
            record
string          forename;
string          surname;
udecimal8       dob;
udecimal8       booklimit := 0;
unsigned2       aage := 0;
            end;

sqBookRec := 
            record
string          name;
string          author;
unsigned1       rating100;
udecimal8_2     price := 0;
            end;


// Nested record definitions
sqPersonBookRec :=
            record
sqPersonRec;
dataset(sqBookRec)      books;
            end;

sqHousePersonBookRec :=
            record
sqHouseRec;
dataset(sqPersonBookRec) persons;
            end;


// Record definitions with additional ids

sqHouseIdRec :=
            record
unsigned4       id;
sqHouseRec;
            end;


sqPersonIdRec :=
            record
unsigned4       id;
sqPersonRec;
            end;


sqBookIdRec :=
            record
unsigned4       id;
sqBookRec;
            end;


// Same with parent linking field.

sqPersonRelatedIdRec :=
            record
sqPersonIdRec;
unsigned4       houseid;
            end;


sqBookRelatedIdRec :=
            record
sqBookIdRec;
unsigned4       personid;
            end;


// Nested definitions with additional ids...

sqPersonBookIdRec :=
            record
sqPersonIdRec;
dataset(sqBookIdRec)        books;
            end;

sqHousePersonBookIdRec :=
            record
sqHouseIdRec;
dataset(sqPersonBookIdRec) persons;
            end;


sqPersonBookRelatedIdRec := 
            RECORD
                sqPersonBookIdRec;
unsigned4       houseid;
            END;

sqNestedBlob := 
            RECORD
udecimal8       booklimit := 0;
            END;

sqSimplePersonBookRec := 
            RECORD
string20        surname;
string10        forename;
udecimal8       dob;
//udecimal8     booklimit := 0;
sqNestedBlob    limit{blob};
unsigned1       aage := 0;
dataset(sqBookIdRec)        books{blob};
            END;
sqNamePrefix := '~REGRESS::' + filePrefix + '::';
sqHousePersonBookName := sqNamePrefix + 'HousePersonBook';
sqPersonBookName := sqNamePrefix + 'PersonBook';
sqHouseName := sqNamePrefix + 'House';
sqPersonName := sqNamePrefix + 'Person';
sqBookName := sqNamePrefix + 'Book';
sqSimplePersonBookName := sqNamePrefix + 'SimplePersonBook';

sqHousePersonBookIndexName := sqNamePrefix + 'HousePersonBookIndex';
sqPersonBookIndexName := sqNamePrefix + 'PersonBookIndex';
sqHouseIndexName := sqNamePrefix + 'HouseIndex';
sqPersonIndexName := sqNamePrefix + 'PersonIndex';
sqBookIndexName := sqNamePrefix + 'BookIndex';
sqSimplePersonBookIndexName := sqNamePrefix + 'SimplePersonBookIndex';
sqHousePersonBookIdExRec := record
sqHousePersonBookIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqPersonBookRelatedIdExRec := record
sqPersonBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqHouseIdExRec := record
sqHouseIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqPersonRelatedIdExRec := record
sqPersonRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqBookRelatedIdExRec := record
sqBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqSimplePersonBookExRec := record
sqSimplePersonBookRec;
unsigned8           filepos{virtual(fileposition)};
                end;

// Dataset definitions:


sqHousePersonBookDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
sqPersonBookDs := dataset(sqPersonBookName, sqPersonBookRelatedIdRec, thor);
sqHouseDs := dataset(sqHouseName, sqHouseIdExRec, thor);
sqPersonDs := dataset(sqPersonName, sqPersonRelatedIdRec, thor);
sqBookDs := dataset(sqBookName, sqBookRelatedIdRec, thor);

sqHousePersonBookExDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
sqPersonBookExDs := dataset(sqPersonBookName, sqPersonBookRelatedIdExRec, thor);
sqHouseExDs := dataset(sqHouseName, sqHouseIdExRec, thor);
sqPersonExDs := dataset(sqPersonName, sqPersonRelatedIdExRec, thor);
sqBookExDs := dataset(sqBookName, sqBookRelatedIdExRec, thor);

sqSimplePersonBookDs := dataset(sqSimplePersonBookName, sqSimplePersonBookExRec, thor);
sqSimplePersonBookIndex := index(sqSimplePersonBookDs, { surname, forename, aage  }, { sqSimplePersonBookDs }, sqSimplePersonBookIndexName);

//related datasets:
//Don't really work because inheritance structure isn't preserved.

relatedBooks(sqPersonIdRec parentPerson) := sqBookDs(personid = parentPerson.id);
relatedPersons(sqHouseIdRec parentHouse) := sqPersonDs(houseid = parentHouse.id);

sqNamesTable1 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);

//UseStandardFiles

o1 := normalize(sqNamesTable1, left.books, transform(right));
o2 := normalize(sqNamesTable1, sort(left.books, -rating100, +author), transform(right));
o3 := normalize(sqNamesTable1, sort(left.books, +author, +name), transform({string name, string author}, self := right));

sequential(
    output(o1), 
    output(o2), 
    output(o3),
    output('done')
);
