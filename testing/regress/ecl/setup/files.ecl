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
//skip type==setup TBD

//define constants
EXPORT files(string p) := module
SHARED DG_GenFlat           := true;   //TRUE gens FlatFile
SHARED DG_GenChild          := true;   //TRUE gens ChildFile
SHARED DG_GenGrandChild     := true;   //TRUE gens GrandChildFile
SHARED DG_GenIndex          := true;   //TRUE gens FlatFile AND the index
SHARED DG_GenCSV            := true;   //TRUE gens CSVFile
SHARED DG_GenXML            := true;   //TRUE gens XMLFile
SHARED DG_GenVar            := true;   //TRUE gens VarFile only IF MaxField >= 3

EXPORT DG_MaxField          := 3;    // maximum number of fields to use building the data records
EXPORT DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs

                    // generates (#parents * DG_MaxChildren) records
EXPORT DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records

SHARED useDynamic := false;
SHARED useLayoutTrans := false;
SHARED useVarIndex := false;
SHARED prefix := 'hthor';

#if (useDynamic=true)
SHARED  VarString EmptyString := '' : STORED('dummy');
SHARED  filePrefix := prefix + EmptyString;
 #option ('allowVariableRoxieFilenames', 1)
#else
SHARED  filePrefix := prefix;
#end

EXPORT DG_FileOut           := '~REGRESS::' + filePrefix + '::DG_';
EXPORT DG_ParentFileOut     := '~REGRESS::' + filePrefix + '::DG_Parent.d00';
EXPORT DG_ChildFileOut      := '~REGRESS::' + filePrefix + '::DG_Child.d00';
EXPORT DG_GrandChildFileOut := '~REGRESS::' + filePrefix + '::DG_GrandChild.d00';
EXPORT DG_FetchFileName     := '~REGRESS::' + filePrefix + '::C.DG_FetchFile';
EXPORT DG_FetchFilePreloadName := '~REGRESS::' + filePrefix + '::C.DG_FetchFilePreload';
EXPORT DG_FetchFilePreloadIndexedName := '~REGRESS::' + filePrefix + '::C.DG_FetchFilePreloadIndexed';
EXPORT DG_FetchIndex1Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex1';
EXPORT DG_FetchIndex2Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex2';
EXPORT DG_FetchIndexDiffName:= '~REGRESS::' + filePrefix + '::DG_FetchIndexDiff';
EXPORT DG_MemFileName       := '~REGRESS::' + filePrefix + '::DG_MemFile';
EXPORT DG_IntegerDatasetName:= '~REGRESS::' + filePrefix + '::DG_IntegerFile';
EXPORT DG_IntegerIndexName  := '~REGRESS::' + filePrefix + '::DG_IntegerIndex';

//record structures
EXPORT DG_FetchRecord := RECORD
  INTEGER8 sequence;
  STRING2  State;
  STRING20 City;
  STRING25 Lname;
  STRING15 Fname;
END;

EXPORT DG_FetchFile   := DATASET(DG_FetchFileName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT);
EXPORT DG_FetchFilePreload := PRELOAD(DATASET(DG_FetchFilePreloadName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT));
EXPORT DG_FetchFilePreloadIndexed := PRELOAD(DATASET(DG_FetchFilePreloadIndexedName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT),1);

#IF (useLayoutTrans=false)
  #IF (useVarIndex=true)
    EXPORT DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
    EXPORT DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
    EXPORT DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{state ,__filepos},DG_FetchIndex1Name);
    EXPORT DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{state, __filepos}, DG_FetchIndex2Name);
  #END
#ELSE
 // Declare all indexes such that layout translation is required... Used at run-time only, not at setup time...
  #IF (useVarIndex=true)
    EXPORT DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
    EXPORT DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
    EXPORT DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{state ,__filepos},DG_FetchIndex1Name);
    EXPORT DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{state, __filepos}, DG_FetchIndex2Name);
  #END
#END
EXPORT DG_OutRec := RECORD
    unsigned4  DG_ParentID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

EXPORT DG_OutRecChild := RECORD
    unsigned4  DG_ParentID;
    unsigned4  DG_ChildID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

EXPORT DG_VarOutRec := RECORD
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;

//DATASET declarations
EXPORT DG_BlankSet := dataset([{0,'','',0}],DG_OutRec);

EXPORT DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_FlatFileEvens := DATASET(DG_FileOut+'FLAT_EVENS',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);

EXPORT DG_indexFile      := INDEX(DG_FlatFile,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
    END,
     RECORD
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX');

EXPORT DG_indexFileEvens := INDEX(DG_FlatFileEvens,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
    END,
    RECORD
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX_EVENS');

EXPORT DG_CSVFile   := DATASET(DG_FileOut+'CSV',DG_OutRec,CSV);
EXPORT DG_XMLFile   := DATASET(DG_FileOut+'XML',DG_OutRec,XML);

EXPORT DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

EXPORT DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT);
EXPORT DG_VarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},DG_FileOut+'VARINDEX');
EXPORT DG_VarVarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},{ string temp_blob1 := TRIM(ExtraField); string10000 temp_blob2 {blob} := ExtraField },DG_FileOut+'VARVARINDEX');

EXPORT DG_ParentFile  := DATASET(DG_ParentFileOut,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_ChildFile   := DATASET(DG_ChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_GrandChildFile := DATASET(DG_GrandChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);

//define data atoms - each set has 16 elements
EXPORT SET OF STRING10 DG_Fnames := ['DAVID','CLAIRE','KELLY','KIMBERLY','PAMELA','JEFFREY','MATTHEW','LUKE',
                              'JOHN' ,'EDWARD','CHAD' ,'KEVIN'   ,'KOBE'  ,'RICHARD','GEORGE' ,'DIRK'];
EXPORT SET OF STRING10 DG_Lnames := ['BAYLISS','DOLSON','BILLINGTON','SMITH'   ,'JONES'   ,'ARMSTRONG','LINDHORFF','SIMMONS',
                              'WYMAN'  ,'MORTON','MIDDLETON' ,'NOWITZKI','WILLIAMS','TAYLOR'   ,'CHAPMAN'  ,'BRYANT'];
EXPORT SET OF UNSIGNED1 DG_PrangeS := [1, 2, 3, 4, 5, 6, 7, 8,
                                9,10,11,12,13,14,15,16];
EXPORT SET OF STRING10 DG_Streets := ['HIGH'  ,'CITATION'  ,'MILL','25TH' ,'ELGIN'    ,'VICARAGE','YAMATO' ,'HILLSBORO',
                               'SILVER','KENSINGTON','MAIN','EATON','PARK LANE','HIGH'    ,'POTOMAC','GLADES'];
EXPORT SET OF UNSIGNED1 DG_ZIPS := [101,102,103,104,105,106,107,108,
                             109,110,111,112,113,114,115,116];
EXPORT SET OF UNSIGNED1 DG_AGES := [31,32,33,34,35,36,37,38,
                             39,40,41,42,43,44,45,56];
EXPORT SET OF STRING2 DG_STATES := ['FL','GA','SC','NC','TX','AL','MS','TN',
                             'KY','CA','MI','OH','IN','IL','WI','MN'];
EXPORT SET OF STRING3 DG_MONTHS := ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG',
                             'SEP','OCT','NOV','DEC','ABC','DEF','GHI','JKL'];

EXPORT t_personfile := DATASET('t_personfile', RECORD
  unsigned integer4 hhid;
  unsigned integer4 personid;
  string20 firstname;
  string20 lastname;
  string20 middlename;
  unsigned integer1 age;
  unsigned integer8 ssn;
END, THOR);

EXPORT t_tradesfile := DATASET('t_tradesfile', RECORD
  unsigned integer4 personid;
  string20 tradeid;
  real4 amount;
  string8 date;
END, THOR);

EXPORT t_hhfile := DATASET('t_hhfile', RECORD
  unsigned integer4 hhid;
  string2 State;
  string5 zip;
  string20 City;
  string40 street;
  unsigned integer4 houseNumber;
END, THOR);


//----------------------------- Child query related definitions ----------------------------------

// Raw record definitions:

EXPORT sqHouseRec :=
            record
string          addr;
string10        postcode;
unsigned2       yearBuilt := 0;
            end;


EXPORT sqPersonRec :=
            record
string          forename;
string          surname;
udecimal8       dob;
udecimal8       booklimit := 0;
unsigned2       aage := 0;
            end;

EXPORT sqBookRec :=
            record
string          name;
string          author;
unsigned1       rating100;
udecimal8_2     price := 0;
            end;


// Nested record definitions
EXPORT sqPersonBookRec :=
            record
sqPersonRec;
dataset(sqBookRec)      books;
            end;

EXPORT sqHousePersonBookRec :=
            record
sqHouseRec;
dataset(sqPersonBookRec) persons;
            end;


// Record definitions with additional ids

EXPORT sqHouseIdRec :=
            record
unsigned4       id;
sqHouseRec;
            end;


EXPORT sqPersonIdRec :=
            record
unsigned4       id;
sqPersonRec;
            end;


EXPORT sqBookIdRec :=
            record
unsigned4       id;
sqBookRec;
            end;


// Same with parent linking field.

EXPORT sqPersonRelatedIdRec :=
            record
sqPersonIdRec;
unsigned4       houseid;
            end;


EXPORT sqBookRelatedIdRec :=
            record
sqBookIdRec;
unsigned4       personid;
            end;


// Nested definitions with additional ids...

EXPORT sqPersonBookIdRec :=
            record
sqPersonIdRec;
dataset(sqBookIdRec)        books;
            end;

EXPORT sqHousePersonBookIdRec :=
            record
sqHouseIdRec;
dataset(sqPersonBookIdRec) persons;
            end;


EXPORT sqPersonBookRelatedIdRec :=
            RECORD
                sqPersonBookIdRec;
unsigned4       houseid;
            END;

EXPORT sqNestedBlob :=
            RECORD
udecimal8       booklimit := 0;
            END;

EXPORT sqSimplePersonBookRec :=
            RECORD
string20        surname;
string10        forename;
udecimal8       dob;
//udecimal8     booklimit := 0;
sqNestedBlob    limit{blob};
unsigned1       aage := 0;
dataset(sqBookIdRec)        books{blob};
            END;
EXPORT sqNamePrefix := '~REGRESS::' + filePrefix + '::';
EXPORT sqHousePersonBookName := sqNamePrefix + 'HousePersonBook';
EXPORT sqPersonBookName := sqNamePrefix + 'PersonBook';
EXPORT sqHouseName := sqNamePrefix + 'House';
EXPORT sqPersonName := sqNamePrefix + 'Person';
EXPORT sqBookName := sqNamePrefix + 'Book';
EXPORT sqSimplePersonBookName := sqNamePrefix + 'SimplePersonBook';

EXPORT sqHousePersonBookIndexName := sqNamePrefix + 'HousePersonBookIndex';
EXPORT sqPersonBookIndexName := sqNamePrefix + 'PersonBookIndex';
EXPORT sqHouseIndexName := sqNamePrefix + 'HouseIndex';
EXPORT sqPersonIndexName := sqNamePrefix + 'PersonIndex';
EXPORT sqBookIndexName := sqNamePrefix + 'BookIndex';
EXPORT sqSimplePersonBookIndexName := sqNamePrefix + 'SimplePersonBookIndex';
EXPORT sqHousePersonBookIdExRec := record
sqHousePersonBookIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT sqPersonBookRelatedIdExRec := record
sqPersonBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT sqHouseIdExRec := record
sqHouseIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT sqPersonRelatedIdExRec := record
sqPersonRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT sqBookRelatedIdExRec := record
sqBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

EXPORT sqSimplePersonBookExRec := record
sqSimplePersonBookRec;
unsigned8           filepos{virtual(fileposition)};
                end;

// Dataset definitions:


EXPORT sqHousePersonBookDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
EXPORT sqPersonBookDs := dataset(sqPersonBookName, sqPersonBookRelatedIdRec, thor);
EXPORT sqHouseDs := dataset(sqHouseName, sqHouseIdExRec, thor);
EXPORT sqPersonDs := dataset(sqPersonName, sqPersonRelatedIdRec, thor);
EXPORT sqBookDs := dataset(sqBookName, sqBookRelatedIdRec, thor);

EXPORT sqHousePersonBookExDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
EXPORT sqPersonBookExDs := dataset(sqPersonBookName, sqPersonBookRelatedIdExRec, thor);
EXPORT sqHouseExDs := dataset(sqHouseName, sqHouseIdExRec, thor);
EXPORT sqPersonExDs := dataset(sqPersonName, sqPersonRelatedIdExRec, thor);
EXPORT sqBookExDs := dataset(sqBookName, sqBookRelatedIdExRec, thor);

EXPORT sqSimplePersonBookDs := dataset(sqSimplePersonBookName, sqSimplePersonBookExRec, thor);
EXPORT sqSimplePersonBookIndex := index(sqSimplePersonBookDs, { surname, forename, aage  }, { sqSimplePersonBookDs }, sqSimplePersonBookIndexName);

//related datasets:
//Don't really work because inheritance structure isn't preserved.

EXPORT relatedBooks(sqPersonIdRec parentPerson) := sqBookDs(personid = parentPerson.id);
EXPORT relatedPersons(sqHouseIdRec parentHouse) := sqPersonDs(houseid = parentHouse.id);

EXPORT sqNamesTable1 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable2 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable3 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable4 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable5 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable6 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable7 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable8 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
EXPORT sqNamesTable9 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);

EXPORT sqNamesIndex1 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex2 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex3 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex4 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex5 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex6 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex7 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex8 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
EXPORT sqNamesIndex9 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);


//----------------------------- Text search definitions ----------------------------------
EXPORT TS_MaxTerms             := 50;
EXPORT TS_MaxStages            := 50;
EXPORT TS_MaxProximity         := 10;
EXPORT TS_MaxWildcard          := 1000;
EXPORT TS_MaxMatchPerDocument  := 1000;
EXPORT TS_MaxFilenameLength        := 255;
EXPORT TS_MaxActions           := 255;
EXPORT TS_MaxTagNesting        := 40;
EXPORT TS_MaxColumnsPerLine := 10000;          // used to create a pseudo document position

EXPORT TS_kindType         := enum(unsigned1, UnknownEntry=0, TextEntry, OpenTagEntry, CloseTagEntry, OpenCloseTagEntry, CloseOpenTagEntry);
EXPORT TS_sourceType       := unsigned2;
EXPORT TS_wordCountType    := unsigned8;
EXPORT TS_segmentType      := unsigned1;
EXPORT TS_wordPosType      := unsigned8;
EXPORT TS_docPosType       := unsigned8;
EXPORT TS_documentId       := unsigned8;
EXPORT TS_termType         := unsigned1;
EXPORT TS_distanceType     := integer8;
EXPORT TS_indexWipType     := unsigned1;
EXPORT TS_wipType          := unsigned8;
EXPORT TS_stageType        := unsigned1;
EXPORT TS_dateType         := unsigned8;

EXPORT TS_sourceType TS_docid2source(TS_documentId x) := (x >> 48);
EXPORT TS_documentId TS_docid2doc(TS_documentId x) := (x & 0xFFFFFFFFFFFF);
EXPORT TS_documentId TS_createDocId(TS_sourceType source, TS_documentId doc) := (TS_documentId)(((unsigned8)source << 48) | doc);
EXPORT boolean      TS_docMatchesSource(TS_documentId docid, TS_sourceType source) := (docid between TS_createDocId(source,0) and (TS_documentId)(TS_createDocId(source+1,0)-1));

EXPORT TS_wordType := string20;
EXPORT TS_wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);

EXPORT TS_wordIdType       := unsigned4;

EXPORT TS_NameWordIndex        := '~REGRESS::' + filePrefix + '::TS_wordIndex';
EXPORT TS_NameSearchIndex      := '~REGRESS::' + filePrefix + '::TS_searchIndex';

EXPORT TS_wordIndex        := index({ TS_kindType kind, TS_wordType word, TS_documentId doc, TS_segmentType segment, TS_wordPosType wpos, TS_indexWipType wip } , { TS_wordFlags flags, TS_wordType original, TS_docPosType dpos}, TS_NameWordIndex);
EXPORT TS_searchIndex      := index(TS_wordIndex, TS_NameSearchIndex);

EXPORT TS_wordIndexRecord := recordof(TS_wordIndex);

//----------------------------- End of text search definitions --------------------------



EXPORT DG_MemFileRec := RECORD
    unsigned2 u2;
    unsigned3 u3;
    big_endian unsigned2 bu2;
    big_endian unsigned3 bu3;
    integer2 i2;
    integer3 i3;
    big_endian integer2 bi2;
    big_endian integer3 bi3;
END;

EXPORT DG_MemFile := DATASET(DG_MemFileName,DG_MemFileRec,FLAT);


//record structures
EXPORT DG_NestedIntegerRecord := RECORD
  big_endian UNSIGNED4 i4;
  big_endian UNSIGNED3 u3;
END;

EXPORT DG_IntegerRecord := RECORD
    INTEGER6    i6;
    DG_NestedIntegerRecord nested;
    integer5    i5;
    integer3    i3;
END;

EXPORT DG_IntegerDataset := DATASET(DG_IntegerDatasetName, DG_IntegerRecord, thor);
EXPORT DG_IntegerIndex := INDEX(DG_IntegerDataset, { i6, nested }, { DG_IntegerDataset }, DG_IntegerIndexName);

END;
