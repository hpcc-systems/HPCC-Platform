//This is a module which is imported and used from other queries - don't execute it.
//skip type==setup TBD

EXPORT sq(string platform) := MODULE

//MORE: This is currently hard-wired to hthor since it is a tiny dataset
EXPORT NamePrefix := '~REGRESS::' + platform + '::';

EXPORT HouseRec :=
            record
string          addr;
string10        postcode;
unsigned2       yearBuilt := 0;
            end;


EXPORT PersonRec :=
            record
string          forename;
string          surname;
udecimal8       dob;
udecimal8       booklimit := 0;
unsigned2       aage := 0;
            end;

EXPORT BookRec :=
            record
string          name;
string          author;
unsigned1       rating100;
udecimal8_2     price := 0;
            end;


// Nested record definitions
EXPORT PersonBookRec :=
            record
PersonRec;
dataset(BookRec)      books;
            end;

EXPORT HousePersonBookRec := RECORD
    HouseRec;
    dataset(PersonBookRec) persons;
END;


// Record definitions with additional ids

EXPORT HouseIdRec :=
            record
unsigned4       id;
HouseRec;
            end;


EXPORT PersonIdRec :=
            record
unsigned4       id;
PersonRec;
            end;


EXPORT BookIdRec :=
            record
unsigned4       id;
BookRec;
            end;


// Same with parent linking field.

EXPORT PersonRelatedIdRec :=
            record
PersonIdRec;
unsigned4       houseid;
            end;


EXPORT BookRelatedIdRec :=
            record
BookIdRec;
unsigned4       personid;
            end;


// Nested definitions with additional ids...

EXPORT PersonBookIdRec :=
            record
PersonIdRec;
dataset(BookIdRec)        books;
            end;

EXPORT HousePersonBookIdRec :=
            record
HouseIdRec;
dataset(PersonBookIdRec) persons;
            end;


EXPORT PersonBookRelatedIdRec :=
            RECORD
                PersonBookIdRec;
unsigned4       houseid;
            END;

EXPORT NestedBlob :=
            RECORD
udecimal8       booklimit := 0;
            END;

EXPORT SimplePersonBookRec :=
            RECORD
string20        surname;
string10        forename;
udecimal8       dob;
//udecimal8     booklimit := 0;
NestedBlob    limit{blob};
unsigned1       aage := 0;
dataset(BookIdRec)        books{blob};
            END;


EXPORT HousePersonBookIdExRec := record
HousePersonBookIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT PersonBookRelatedIdExRec := record
PersonBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT HouseIdExRec := record
HouseIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT PersonRelatedIdExRec := record
PersonRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT BookRelatedIdExRec := record
BookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT SimplePersonBookExRec := record
SimplePersonBookRec;
unsigned8           filepos{virtual(fileposition)};
                end;

// Dataset definitions:


EXPORT HousePersonBookName := NamePrefix + 'HousePersonBook';
EXPORT PersonBookName := NamePrefix + 'PersonBook';
EXPORT HouseName := NamePrefix + 'House';
EXPORT PersonName := NamePrefix + 'Person';
EXPORT BookName := NamePrefix + 'Book';
EXPORT SimplePersonBookName := NamePrefix + 'SimplePersonBook';

EXPORT HousePersonBookIndexName := NamePrefix + 'HousePersonBookIndex';
EXPORT PersonBookIndexName := NamePrefix + 'PersonBookIndex';
EXPORT HouseIndexName := NamePrefix + 'HouseIndex';
EXPORT PersonIndexName := NamePrefix + 'PersonIndex';
EXPORT BookIndexName := NamePrefix + 'BookIndex';
EXPORT SimplePersonBookIndexName := NamePrefix + 'SimplePersonBookIndex';

EXPORT HousePersonBookDs := dataset(HousePersonBookName, HousePersonBookIdExRec, thor);
EXPORT PersonBookDs := dataset(PersonBookName, PersonBookRelatedIdRec, thor);
EXPORT HouseDs := dataset(HouseName, HouseIdExRec, thor);
EXPORT PersonDs := dataset(PersonName, PersonRelatedIdRec, thor);
EXPORT BookDs := dataset(BookName, BookRelatedIdRec, thor);

EXPORT HousePersonBookExDs := dataset(HousePersonBookName, HousePersonBookIdExRec, thor);
EXPORT PersonBookExDs := dataset(PersonBookName, PersonBookRelatedIdExRec, thor);
EXPORT HouseExDs := dataset(HouseName, HouseIdExRec, thor);
EXPORT PersonExDs := dataset(PersonName, PersonRelatedIdExRec, thor);
EXPORT BookExDs := dataset(BookName, BookRelatedIdExRec, thor);

EXPORT SimplePersonBookDs := dataset(SimplePersonBookName, SimplePersonBookExRec, thor);
EXPORT SimplePersonBookIndex := index(SimplePersonBookDs, { surname, forename, aage  }, { SimplePersonBookDs }, SimplePersonBookIndexName);

//related datasets:
//Don't really work because inheritance structure isn't preserved.

EXPORT relatedBooks(PersonIdRec parentPerson) := BookDs(personid = parentPerson.id);
EXPORT relatedPersons(HouseIdRec parentHouse) := PersonDs(houseid = parentHouse.id);

EXPORT NamesTable1 := dataset(SimplePersonBookDs, SimplePersonBookName, FLAT);
EXPORT NamesIndex1 := index(SimplePersonBookIndex,SimplePersonBookIndexName);

END;
