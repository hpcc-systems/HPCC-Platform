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
#option('targetClusterType', 'hthor');
#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',false);
#option ('optimizeIndexSource',true);
#option ('optimizeThorCounts',false);
#option ('countIndex',false);

import std.system.thorlib,lib_stringlib;
prefix := 'hthor';

//define constants
filePrefix := prefix;

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


//sort order is deliberately different from anything that will be used later
xpersons := sort(sqHousePersonBookDs.persons, surname + (string)dob + forename)[1..200];
xpersonByAgeDesc := sort(xpersons, dob);
xxbooks := sort(xpersons.books, name + (string)rating100 + author)[1..200];
xxbooksByRatingDesc := sort(xxbooks, -rating100);

output(sqHousePersonBookDs, { addr, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
