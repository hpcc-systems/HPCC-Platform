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

MODULE

export declareCommon() := macro

#option ('optimizeDiskSource',true);
#option ('optimizeThorCounts',false);
#option ('optimizeChildSource',true);

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

sqDirectory := 'sq';
sqNamePrefix := '~' + sqDirectory + IF(sqDirectory <> '', '::', '');

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
string40            filename{virtual(logicalfilename)};
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

endmacro;

END;
