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

#option ('targetClusterType', 'roxie');
#option ('tempDatasetsUseLinkedRows', true);
#option ('implicitLinkedChildRows', true);

import lib_stringlib,std.system.thorlib;
prefix := 'hthor';
useLayoutTrans := false;
useLocal := false;
usePayload := false;
useVarIndex := false;
useDynamic := false;
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

DG_FileOut           := '~REGRESS::' + filePrefix + '::DG_';
DG_ParentFileOut     := '~REGRESS::' + filePrefix + '::DG_Parent.d00';
DG_ChildFileOut      := '~REGRESS::' + filePrefix + '::DG_Child.d00';
DG_GrandChildFileOut := '~REGRESS::' + filePrefix + '::DG_GrandChild.d00';
DG_FetchFileName     := '~REGRESS::' + filePrefix + '::DG_FetchFile';
DG_FetchFilePreloadName := '~REGRESS::' + filePrefix + '::DG_FetchFilePreload';
DG_FetchFilePreloadIndexedName := '~REGRESS::' + filePrefix + '::DG_FetchFilePreloadIndexed';
DG_FetchIndex1Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex1';
DG_FetchIndex2Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex2';
DG_FetchIndexDiffName:= '~REGRESS::' + filePrefix + '::DG_FetchIndexDiff';
DG_MemFileName       := '~REGRESS::' + filePrefix + '::DG_MemFile';

//record structures
DG_FetchRecord := RECORD
  INTEGER8 sequence;
  STRING2  State;
  STRING20 City;
  STRING25 Lname;
  STRING15 Fname;
END;

DG_FetchFile   := DATASET(DG_FetchFileName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT);
DG_FetchFilePreload := PRELOAD(DATASET(DG_FetchFilePreloadName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT));
DG_FetchFilePreloadIndexed := PRELOAD(DATASET(DG_FetchFilePreloadIndexedName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT),1);

#IF (useLayoutTrans=false)
 #IF (usePayload=false)
  DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname,__filepos},DG_FetchIndex1Name);
  DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname, __filepos}, DG_FetchIndex2Name);
 #ELSE
  #IF (useVarIndex=true)
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{state ,__filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{state, __filepos}, DG_FetchIndex2Name);
  #END
 #END
#ELSE
 // Declare all indexes such that layout translation is required... Used at run-time only, not at setup time...
 #IF (usePayload=false)
  DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname,__filepos},DG_FetchIndex1Name);
  DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname, __filepos}, DG_FetchIndex2Name);
 #ELSE
  #IF (useVarIndex=true)
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{state ,__filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{state, __filepos}, DG_FetchIndex2Name);
  #END
 #END
#END
DG_OutRec := RECORD
    unsigned4  DG_ParentID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

DG_OutRecChild := RECORD
    unsigned4  DG_ParentID;
    unsigned4  DG_ChildID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

#if(DG_GenVar = TRUE)
DG_VarOutRec := RECORD
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;
#end

//DATASET declarations
DG_BlankSet := dataset([{0,'','',0}],DG_OutRec);

#if(DG_GenFlat = TRUE OR DG_GenIndex = TRUE)
DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_FlatFileEvens := DATASET(DG_FileOut+'FLAT_EVENS',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
#end

#if(DG_GenIndex = TRUE)
DG_indexFile      := INDEX(DG_FlatFile,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
#if(usePayload = TRUE)
    END , RECORD
#end
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX');

DG_indexFileEvens := INDEX(DG_FlatFileEvens,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
#if(usePayload = TRUE)
    END , RECORD
#end
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX_EVENS');
#end

#if(DG_GenCSV = TRUE)
DG_CSVFile   := DATASET(DG_FileOut+'CSV',DG_OutRec,CSV);
#end

#if(DG_GenXML = TRUE)
DG_XMLFile   := DATASET(DG_FileOut+'XML',DG_OutRec,XML);
#end

#if(DG_GenVar = TRUE)
DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT);
DG_VarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},DG_FileOut+'VARINDEX');
DG_VarVarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},{ string temp_blob1 := TRIM(ExtraField); string10000 temp_blob2 {blob} := ExtraField },DG_FileOut+'VARVARINDEX');
#end
#if(DG_GenChild = TRUE)
DG_ParentFile  := DATASET(DG_ParentFileOut,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_ChildFile   := DATASET(DG_ChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #if(DG_GenGrandChild = TRUE)
DG_GrandChildFile := DATASET(DG_GrandChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #end
#end

//define data atoms - each set has 16 elements
SET OF STRING10 DG_Fnames := ['DAVID','CLAIRE','KELLY','KIMBERLY','PAMELA','JEFFREY','MATTHEW','LUKE',
                              'JOHN' ,'EDWARD','CHAD' ,'KEVIN'   ,'KOBE'  ,'RICHARD','GEORGE' ,'DIRK'];
SET OF STRING10 DG_Lnames := ['BAYLISS','DOLSON','BILLINGTON','SMITH'   ,'JONES'   ,'ARMSTRONG','LINDHORFF','SIMMONS',
                              'WYMAN'  ,'MORTON','MIDDLETON' ,'NOWITZKI','WILLIAMS','TAYLOR'   ,'DRIMBAD'  ,'BRYANT'];
SET OF UNSIGNED1 DG_PrangeS := [1, 2, 3, 4, 5, 6, 7, 8,
                                9,10,11,12,13,14,15,16];
SET OF STRING10 DG_Streets := ['HIGH'  ,'CITATION'  ,'MILL','25TH' ,'ELGIN'    ,'VICARAGE','YAMATO' ,'HILLSBORO',
                               'SILVER','KENSINGTON','MAIN','EATON','PARK LANE','HIGH'    ,'POTOMAC','GLADES'];
SET OF UNSIGNED1 DG_ZIPS := [101,102,103,104,105,106,107,108,
                             109,110,111,112,113,114,115,116];
SET OF UNSIGNED1 DG_AGES := [31,32,33,34,35,36,37,38,
                             39,40,41,42,43,44,45,56];
SET OF STRING2 DG_STATES := ['FL','GA','SC','NC','TX','AL','MS','TN',
                             'KY','CA','MI','OH','IN','IL','WI','MN'];
SET OF STRING3 DG_MONTHS := ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG',
                             'SEP','OCT','NOV','DEC','ABC','DEF','GHI','JKL'];


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
sqNamesTable2 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable3 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable4 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable5 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable6 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable7 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable8 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable9 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);

sqNamesIndex1 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex2 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex3 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex4 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex5 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex6 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex7 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex8 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex9 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);


//----------------------------- Text search definitions ----------------------------------
TS_MaxTerms             := 50;
TS_MaxStages            := 50;
TS_MaxProximity         := 10;
TS_MaxWildcard          := 1000;
TS_MaxMatchPerDocument  := 1000;
TS_MaxFilenameLength        := 255;
TS_MaxActions           := 255;
TS_MaxTagNesting        := 40;

TS_kindType         := enum(unsigned1, UnknownEntry=0, TextEntry, OpenTagEntry, CloseTagEntry, OpenCloseTagEntry, CloseOpenTagEntry);
TS_sourceType       := unsigned2;
TS_wordCountType    := unsigned8;
TS_segmentType      := unsigned1;
TS_wordPosType      := unsigned8;
TS_docPosType       := unsigned8;
TS_documentId       := unsigned8;
TS_termType         := unsigned1;
TS_distanceType     := integer8;
TS_indexWipType     := unsigned1;
TS_wipType          := unsigned8;
TS_stageType        := unsigned1;
TS_dateType         := unsigned8;

TS_sourceType TS_docid2source(TS_documentId x) := (x >> 48);
TS_documentId TS_docid2doc(TS_documentId x) := (x & 0xFFFFFFFFFFFF);
TS_documentId TS_createDocId(TS_sourceType source, TS_documentId doc) := (TS_documentId)(((unsigned8)source << 48) | doc);
boolean      TS_docMatchesSource(TS_documentId docid, TS_sourceType source) := (docid between TS_createDocId(source,0) and (TS_documentId)(TS_createDocId(source+1,0)-1));

TS_wordType := string20;
TS_wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);

TS_wordIdType       := unsigned4;

TS_NameWordIndex        := '~REGRESS::' + filePrefix + '::TS_wordIndex';

TS_wordIndex        := index({ TS_kindType kind, TS_wordType word, TS_documentId doc, TS_segmentType segment, TS_wordPosType wpos, TS_indexWipType wip } , { TS_wordFlags flags, TS_wordType original, TS_docPosType dpos}, TS_NameWordIndex);

TS_wordIndexRecord := recordof(TS_wordIndex);

//----------------------------- End of text search definitions --------------------------



DG_MemFileRec := RECORD
    unsigned2 u2;
    unsigned3 u3;
    big_endian unsigned2 bu2;
    big_endian unsigned3 bu3;
    integer2 i2;
    integer3 i3;
    big_endian integer2 bi2;
    big_endian integer3 bi3;
END;

DG_MemFile := DATASET(DG_MemFileName,DG_MemFileRec,FLAT);

#line(0)
//UseStandardFiles
//tidyoutput
//nothor
//UseIndexes
//xxvarskip type==roxie && setuptype==thor && !local

INCLUDE_DEBUG_INFO := false;

LOADXML('<xml/>');
#option ('checkAsserts',false);
import lib_stringLib;

MaxTerms            := TS_MaxTerms;
MaxStages           := TS_MaxStages;
MaxProximity        := TS_MaxProximity;
MaxWildcard     := TS_MaxWildcard;
MaxMatchPerDocument := TS_MaxMatchPerDocument;
MaxFilenameLength := TS_MaxFilenameLength;
MaxActions       := TS_MaxActions;

kindType        := TS_kindType;
sourceType      := TS_sourceType;
wordCountType   := TS_wordCountType;
segmentType     := TS_segmentType;
wordPosType     := TS_wordPosType;
docPosType      := TS_docPosType;
documentId      := TS_documentId;
termType        := TS_termType;
distanceType    := TS_distanceType;
stageType       := TS_stageType;
dateType        := TS_dateType;
matchCountType  := unsigned2;
wordType        := TS_wordType;
wordFlags       := TS_wordFlags;
wordIdType      := TS_wordIdType;

wordIndex := TS_wordIndex;

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
wordIndexRecord := TS_wordIndexRecord;

MaxWipWordOrAlias  := 4;
MaxWipTagContents  := 65535;
MaxWordsInDocument := 1000000;
MaxWordsInSet      := 20;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

actionEnum := ENUM(
    None = 0,

//Minimal operations required to implement the searching.
    ReadWord,           // termNum, source, segment, word, wordFlagMask, wordFlagCompare,
    ReadWordSet,        // termNum, source, segment, words, wordFlagMask, wordFlagCompare,
    AndTerms,           //
    OrTerms,            //
    AndNotTerms,        //
    PhraseAnd,          //
    ProximityAnd,       // distanceBefore, distanceAfter
    MofNTerms,          // minMatches, maxMatches
    RankMergeTerms,     // left outer join
    RollupByDocument,   // grouped rollup by document.
    NormalizeMatch,     // Normalize proximity records.
    Phrase1To5And,      // For testing range limits
    GlobalAtLeast,
    ContainedAtLeast,
    TagContainsSearch,  // Used for the outermost IN() expression - check it overlaps and rolls up
    TagContainsTerm,    // Used for an inner tag contains - checks, but doesn't roll up
    TagNotContainsTerm, //
    SameContainer,
    NotSameContainer,   //
    MofNContainer,      //
    RankContainer,      // NOTIMPLEMENTED
    OverlapProximityAnd,

//The following aren't very sensible as far as text searching goes, but are here to test the underlying functionality
    AndJoinTerms,       // join on non-proximity
    AndNotJoinTerms,    //
    MofNJoinTerms,      // minMatches, maxMatches
    RankJoinTerms,      // left outer join
    ProximityMergeAnd,  // merge join on proximity
    RollupContainer,
    PositionFilter,     // a filter on position - which will cause lots of rows to be skipped.

//Possibly sensible
    ChooseRange,
    ButNotTerms,
    ButNotJoinTerms,

    PassThrough,
    Last,

    //The following are only used in the production
    FlagModifier,       // wordFlagMask, wordFlagCompare
    QuoteModifier,      //
    Max
);

//  FAIL(stageType, 'Missing entry: ' + (string)action));

boolean definesTerm(actionEnum action) :=
    (action in [actionEnum.ReadWord, actionEnum.ReadWordSet]);

booleanRecord := { boolean value };
stageRecord := { stageType stage };
termRecord := { termType term };
stageMapRecord := { stageType from; stageType to };
wipRecord := { wordPosType wip; };
wordRecord := { wordType word; };
wordSet := set of wordType;
stageSet := set of stageType;

createStage(stageType stage) := transform(stageRecord, self.stage := stage);
createTerm(termType term) := transform(termRecord, self.term := term);

//should have an option to optimize the order
searchRecord :=
            RECORD  //,PACK
stageType       stage;
termType        term;
actionEnum      action;

dataset(stageRecord) inputs{maxcount(MaxStages)};

distanceType    maxWip;
distanceType    maxWipChild;
distanceType    maxWipLeft;
distanceType    maxWipRight;

//The item being searched for
wordType        word;
dataset(wordRecord) words{maxcount(maxWordsInSet)};
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
sourceType      source;
segmentType     segment;
wordPosType     seekWpos;

//Modifiers for the connector/filter
distanceType    maxDistanceRightBeforeLeft;
distanceType    maxDistanceRightAfterLeft;
matchCountType  minMatches;
matchCountType  maxMatches;
dataset(termRecord) termsToProcess{maxcount(MaxTerms)};     // which terms to count with an atleast

#if (INCLUDE_DEBUG_INFO)
string          debug{maxlength(200)}
#end
            END;

searchDataset := DATASET(searchRecord);

childMatchRecord := RECORD
wordPosType         wpos;
wordPosType         wip;
termType            term;               // slightly different from the stage - since stages can get transformed.
                END;


simpleUserOutputRecord :=
        record
unsigned2           source;
unsigned6           subDoc;
wordPosType         wpos;
wordPosType         wip;
dataset(childMatchRecord) words{maxcount(MaxProximity)};
        end;



StageSetToDataset(stageSet x) := dataset(x, stageRecord);
StageDatasetToSet(dataset(stageRecord) x) := set(x, stage);

hasSingleRowPerMatch(actionEnum kind) :=
    (kind IN [  actionEnum.ReadWord,
                actionEnum.ReadWordSet,
                actionEnum.PhraseAnd,
                actionEnum.ProximityAnd,
                actionEnum.ContainedAtLeast,
                actionEnum.TagContainsTerm,
                actionEnum.TagContainsSearch,
                actionEnum.OverlapProximityAnd]);

inheritsSingleRowPerMatch(actionEnum kind) :=
    (kind IN [  actionEnum.OrTerms,
//              actionEnum.AndNotTerms,                 // move container inside an andnot
                actionEnum.TagNotContainsTerm,
                actionEnum.NotSameContainer]);

string1 TF(boolean value) := IF(value, 'T', 'F');

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matches

matchRecord :=  RECORD
documentId          doc;
segmentType         segment;
wordPosType         wpos;
wordPosType         wip;
stageType           term;
dataset(childMatchRecord) children{maxcount(MaxProximity)};
                END;

createChildMatch(wordPosType wpos, wordPosType wip, termType term) := transform(childMatchRecord, self.wpos := wpos; self.wip := wip; self.term := term);
SetOfInputs := set of dataset(matchRecord);

//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//---------------------------------------- Code for executing queries -----------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------

createChildrenFromMatch(matchRecord l) := function
    rawChildren := IF(exists(l.children), l.children, dataset(row(createChildMatch(l.wpos, l.wip, l.term))));
    sortedChildren := sorted(rawChildren, wpos, wip, assert);
    return sortedChildren;
end;

combineChildren(dataset(childMatchRecord) l, dataset(childMatchRecord) r) := function
    lSorted := sorted(l, wpos, wip, assert);
    rSorted := sorted(r, wpos, wip, assert);
    mergedDs := merge(lSorted, rSorted, sorted(wpos, wip, term));
    return dedup(mergedDs, wpos, wip, term);
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching helper functions

matchSearchFlags(wordIndex wIndex, searchRecord search) :=
    keyed(search.segment = 0 or wIndex.segment = search.segment, opt) AND
    ((wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);

matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.kind = kindType.TextEntry and wIndex.word = search.word) AND
    matchSearchFlags(wIndex, search);

matchManyWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.kind = kindType.TextEntry and wIndex.word in set(search.words, word)) AND
    matchSearchFlags(wIndex, search);

matchSearchSource(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR TS_docMatchesSource(wIndex.doc, search.source), opt);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWord(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchSingleWord(wordIndex, search) AND
                        matchSearchSource(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.term := search.term;
        self := [];
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left), hint(dontDuplicateMe));

    return projected;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWordSet(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchManyWord(wordIndex, search) AND
                        matchSearchSource(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.term := search.term;
        self := [];
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left));

    return projected;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// OrTerms

doOrTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return merge(inputs, doc, segment, wpos, dedup);        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndTerms

doAndTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, dedup);     // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotTerms

doAndNotTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ButNotTerms

doButNotTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc and left.segment=right.segment) and
                             (LEFT.wpos BETWEEN RIGHT.wpos AND RIGHT.wpos+RIGHT.wip), doc, segment, wpos, left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ButNotJoinTerms

doButNotJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc and left.segment=right.segment) and
                             (LEFT.wpos BETWEEN RIGHT.wpos AND RIGHT.wpos+RIGHT.wip), transform(left), sorted(doc, segment, wpos), left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RankMergeTerms

doRankMergeTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// MofN mergejoin

doMofNTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, dedup, mofn(search.minMatches, search.maxMatches));     // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Join varieties - primarily for testing

//Note this testing transform wouldn't work correctly with proximity operators as inputs.
matchRecord createDenormalizedMatch(searchRecord search, matchRecord l, dataset(matchRecord) matches) := transform

    wpos := min(matches, wpos);
    wend := max(matches, wpos + wip);

    self.wpos := wpos;
    self.wip := wend - wpos;
    self.children := sort(normalize(matches, 1, createChildMatch(LEFT.wpos, LEFT.wip, LEFT.term)), wpos, wip, term);
    self.term := search.term;
    self := l;
end;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndJoinTerms

doAndJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc) and (left.wpos <> right.wpos), createDenormalizedMatch(search, LEFT, ROWS(left)), sorted(doc, segment, wpos));
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotJoinTerms

doAndNotJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(search, LEFT, ROWS(left)), sorted(doc, segment, wpos), left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RankJoinTerms

doRankJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(search, LEFT, ROWS(left)), sorted(doc, segment, wpos), left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// MofN Join

doMofNJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(search, LEFT, ROWS(left)), sorted(doc, segment, wpos), mofn(search.minMatches, search.maxMatches));
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// PhraseAnd

steppedPhraseCondition(matchRecord l, matchRecord r, distanceType maxWip) :=
        (l.doc = r.doc) and (l.segment = r.segment) and
        (r.wpos between l.wpos+1 and l.wpos+maxWip);

doPhraseAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedPhraseCondition(l, r, search.maxWipLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos = l.wpos + l.wip);

    matchRecord createMatch(matchRecord l, dataset(matchRecord) allRows) := transform
        self.wip := sum(allRows, wip);
        self.term := search.term;
        self := l;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, ROWS(LEFT)), sorted(doc, segment, wpos));

    return matches;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// PhraseAnd

steppedPhrase1To5Condition(matchRecord l, matchRecord r, distanceType maxWip) :=
        (l.doc = r.doc) and (l.segment = r.segment) and
        (r.wpos between l.wpos+1 and l.wpos+5);

doPhrase1To5And(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedPhrase1To5Condition(l, r, search.maxWipLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos = l.wpos + l.wip);

    matchRecord createMatch(matchRecord l, dataset(matchRecord) allRows) := transform
        self.wip := sum(allRows, wip);
        self.term := search.term;
        self := l;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, ROWS(LEFT)), sorted(doc, segment, wpos));

    return matches;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// GlobalAtLeast

doGlobalAtLeast(searchRecord search, SetOfInputs inputs) := FUNCTION
    input := inputs[1];
    groupedInput := group(input, doc);
    filtered := having(groupedInput, count(rows(left)) >= search.minMatches);
    return group(filtered);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ContainedAtLeast

//if container count the number of entries for each unique container == each unique (wpos)
//may possibly be issues with multiple containers starting at the same position
doContainedAtLeastV1(searchRecord search, SetOfInputs inputs) := FUNCTION
    input := inputs[1];
    groupedInput := group(input, doc, segment, wpos);
    filtered := having(groupedInput, count(rows(left)) >= search.minMatches);
    return group(filtered);
END;


doContainedAtLeast(searchRecord search, SetOfInputs inputs) := FUNCTION
    input := inputs[1];

    return input(count(children(term in set(search.termsToProcess, term))) >= search.minMatches);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// SameContainer

matchRecord mergeContainers(termType term, matchRecord l, matchRecord r) := transform
    leftChildren := sorted(l.children, wpos, wip, assert);
    rightChildren := sorted(r.children, wpos, wip, assert);
    self.children := merge(leftChildren, rightChildren, sorted(wpos, wip, term), dedup);
    self.term := term;
    self := l;
end;


doSameContainerOld(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc and left.segment = right.segment and left.wpos = right.wpos) and (left.wip = right.wip), mergeContainers(search.term, LEFT, RIGHT), sorted(doc, segment, wpos));
END;

doSameContainer(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc and left.segment = right.segment and left.wpos = right.wpos) and (left.wip = right.wip), sorted(doc, segment, wpos));
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotSameContainer

doNotSameContainer(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc and left.segment = right.segment and left.wpos = right.wpos) and (left.wip = right.wip), doc, segment, wpos, left only);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// M of N container

doMofNContainer(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc and left.segment = right.segment and left.wpos = right.wpos) and (left.wip = right.wip), doc, segment, wpos, mofn(search.minMatches, search.maxMatches));     // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadContainer  (used internally by in/notin)

doReadContainer(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        keyed(kind = kindType.OpenTagEntry and word = search.word) AND
                        matchSearchFlags(wordIndex, search) AND
                        matchSearchSource(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.term := search.term;
        self := []
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left));

    return projected;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RollupContainer - used for in transformation of TagContainsSearch(a and b)

rollupContainerContents(searchRecord search, dataset(matchRecord) input) := FUNCTION
    groupedByPosition := group(input, doc, segment, wpos);

    matchRecord combine(matchRecord l, dataset(matchRecord) matches) := transform

        //each child record already contains an entry for the container.
        //ideally we want an nary-merge,dedup to combine the children
        //self.children := merge(SET(matches, children), wpos, wip, term, dedup);       if we had the syntax
        allMatches := sort(matches.children, wpos, wip, term);
        self.children := dedup(allMatches, wpos, wip, term);
        self.term := search.term;
        self := l;
    end;
    return rollup(groupedByPosition, group, combine(LEFT, ROWS(LEFT)));
END;

doRollupContainer(searchRecord search, SetOfInputs inputs) := FUNCTION
    return rollupContainerContents(search, inputs[1]);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
//PositionFilter

doPositionFilter(searchRecord search, SetOfInputs inputs) := FUNCTION
    return (inputs[1])(wpos = search.seekWpos);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
//PositionFilter

doChooseRange(searchRecord search, SetOfInputs inputs) := FUNCTION
    return choosen(inputs[1], search.maxMatches - search.minMatches + 1, search.minMatches);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// TagContainsTerm      - used for an element within a complex container expression

//MORE: This should probably always add container, term, and children of term, so that entries for proximity operators are
//      included in the children list - so that atleast on a proximity can be implemented correctly.

matchRecord combineContainer(searchRecord search, matchRecord container, matchRecord terms) := transform
    containerEntry := row(transform(childMatchRecord, self.wpos := container.wpos; self.wip := container.wip; self.term := container.term));
    SELF.children := combineChildren(dataset(containerEntry), createChildrenFromMatch(terms));
    self.term := search.term;
    self := container;
end;

boolean isTermInsideTag(matchRecord term, matchRecord container) :=
        STEPPED(term.doc = container.doc and term.segment = container.segment) and
                ((term.wpos >= container.wpos) and (term.wpos + term.wip <= container.wpos + container.wip));


doTagContainsTerm(searchRecord search, SetOfInputs inputs) := FUNCTION
    matchedTermInput := inputs[1];
    containerInput := doReadContainer(search);
    combined := join([matchedTermInput, containerInput],    isTermInsideTag(left, right),
                combineContainer(search, RIGHT, LEFT), sorted(doc, segment, wpos));
    return combined;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// TagContainsSearch

doTagContainsSearch(searchRecord search, SetOfInputs inputs) := FUNCTION
    return rollupContainerContents(search, doTagContainsTerm(search, inputs));
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// TagNotContainsTerm

doTagNotContainsTerm(searchRecord search, SetOfInputs inputs) := FUNCTION
    matchedTermInput := inputs[1];
    containerInput := doReadContainer(search);
    return mergejoin([matchedTermInput, containerInput], isTermInsideTag(left, right), sorted(doc, segment, wpos), left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ProximityAnd

steppedProximityCondition(matchRecord l, matchRecord r, distanceType maxWipLeft, distanceType maxWipRight, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) := function
        // if maxDistanceRightBeforeLeft is < 0 it means it must follow, so don't add maxWipRight
        maxRightBeforeLeft := IF(maxDistanceRightBeforeLeft >= 0, maxDistanceRightBeforeLeft + maxWipRight, maxDistanceRightBeforeLeft);
        maxRightAfterLeft := IF(maxDistanceRightAfterLeft >= 0, maxDistanceRightAfterLeft + maxWipLeft, maxDistanceRightAfterLeft);

        return
            (l.doc = r.doc) and (l.segment = r.segment) and
            (r.wpos + maxRightBeforeLeft >= l.wpos) and             // (right.wpos + right.wip + maxRightBeforeLeft >= left.wpos)
            (r.wpos <= l.wpos + (maxRightAfterLeft));               // (right.wpos <= left.wpos + left.wip + maxRightAfterLeft)
end;


doProximityAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedProximityCondition(l, r, search.maxWipLeft, search.maxWipRight, search.maxDistanceRightBeforeLeft, search.maxDistanceRightAfterLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos + r.wip + search.maxDistanceRightBeforeLeft >= l.wpos) and
        (r.wpos <= l.wpos + l.wip + search.maxDistanceRightAfterLeft);

    overlaps(wordPosType wpos, childMatchRecord r) := (wpos between r.wpos and r.wpos + (r.wip - 1));

    anyOverlap(childMatchRecord l, childMatchRecord r) :=
                               overlaps(l.wpos, r) or overlaps(l.wpos+(l.wip-1), r) or
                               overlaps(r.wpos, l) or overlaps(r.wpos+(r.wip-1), l);

    createMatch(matchRecord l, matchRecord r) := function

        wpos := min(l.wpos, r.wpos);
        wend := max(l.wpos + l.wip, r.wpos + r.wip);

        leftChildren := createChildrenFromMatch(l);
        rightChildren := createChildrenFromMatch(r);
        //anyOverlaps := exists(join(leftChildren, rightChildren, anyOverlap(left, right), all));
        anyOverlaps := exists(leftChildren(exists(rightChildren(anyOverlap(leftChildren, rightChildren)))));

    //Check for any overlaps between the words, should be disjoint.
        matchRecord matchTransform := transform, skip(anyOverlaps)
            self.wpos := wpos;
            self.wip := wend - wpos;
            self.children := merge(leftChildren, rightChildren, sorted(wpos, wip, term), dedup);
            self.term := search.term;
            self := l;
        end;

        return matchTransform;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, RIGHT), sorted(doc, segment, wpos));

    return matches;
END;


doProximityMergeAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedProximityCondition(l, r, search.maxWipLeft, search.maxWipRight, search.maxDistanceRightBeforeLeft, search.maxDistanceRightAfterLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos + r.wip + search.maxDistanceRightBeforeLeft >= l.wpos) and
        (r.wpos <= l.wpos + l.wip + search.maxDistanceRightAfterLeft);

    overlaps(wordPosType wpos, childMatchRecord r) := (wpos between r.wpos and r.wpos + (r.wip - 1));

    anyOverlaps (matchRecord l, matchRecord r) := function

        wpos := min(l.wpos, r.wpos);
        wend := max(l.wpos + l.wip, r.wpos + r.wip);

        leftChildren := createChildrenFromMatch(l);
        rightChildren := createChildrenFromMatch(r);
        anyOverlaps := exists(join(leftChildren, rightChildren,
                               overlaps(left.wpos, right) or overlaps(left.wpos+(left.wip-1), right) or
                               overlaps(right.wpos, left) or overlaps(right.wpos+(right.wip-1), left), all));

        return anyOverlaps;
    end;

    matches := mergejoin(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT) and not anyOverlaps(LEFT,RIGHT), sorted(doc, segment, wpos));

    return matches;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// OverlapProximityAnd

doOverlapProximityAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) :=
            (l.doc = r.doc) and (l.segment = r.segment) and
            (r.wpos + search.maxWipRight >= l.wpos) and
            (r.wpos <= l.wpos + search.maxWipLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos + r.wip >= l.wpos) and (r.wpos <= l.wpos + l.wip);


    createMatch(matchRecord l, matchRecord r) := function

        wpos := min(l.wpos, r.wpos);
        wend := max(l.wpos + l.wip, r.wpos + r.wip);

        leftChildren := createChildrenFromMatch(l);
        rightChildren := createChildrenFromMatch(r);

        matchRecord matchTransform := transform
            self.wpos := wpos;
            self.wip := wend - wpos;
            self.children := merge(leftChildren, rightChildren, sorted(wpos, wip, term), dedup);
            self.term := search.term;
            self := l;
        end;

        return matchTransform;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, RIGHT), sorted(doc, segment, wpos));

    return matches;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Normalize denormalized proximity records

doNormalizeMatch(searchRecord search, SetOfInputs inputs) := FUNCTION

    matchRecord createNorm(matchRecord l, unsigned c) := transform
        hasChildren := count(l.children) <> 0;
        curChild := l.children[NOBOUNDCHECK c];
        self.wpos := if (hasChildren, curChild.wpos, l.wpos);
        self.wip := if (hasChildren, curChild.wip, l.wip);
        self.term := search.term;
        self.children := [];
        self := l;
    end;

    normalizedRecords := normalize(inputs[1], MAX(1, count(LEFT.children)), createNorm(left, counter));
    groupedNormalized := group(normalizedRecords, doc, segment);
    sortedNormalized := sort(groupedNormalized, wpos, wip);
    dedupedNormalized := dedup(sortedNormalized, wpos, wip);
    return group(dedupedNormalized);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Rollup by document

doRollupByDocument(searchRecord search, dataset(matchRecord) input) := FUNCTION
    groupByDocument := group(input, doc);
    dedupedByDocument := rollup(groupByDocument, group, transform(matchRecord, self.doc := left.doc; self.segment := 0; self.wpos := 0; self.wip := 0; self.term := search.term; self := left));
    return dedupedByDocument;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStage(searchRecord search, SetOfInputs allInputs) := function
    inputs:= RANGE(allInputs, StageDatasetToSet(search.inputs));
    result := case(search.action,
        actionEnum.ReadWord             => doReadWord(search),
        actionEnum.ReadWordSet          => doReadWordSet(search),
        actionEnum.AndTerms             => doAndTerms(search, inputs),
        actionEnum.OrTerms              => doOrTerms(search, inputs),
        actionEnum.AndNotTerms          => doAndNotTerms(search, inputs),
        actionEnum.PhraseAnd            => doPhraseAnd(search, inputs),
        actionEnum.ProximityAnd         => doProximityAnd(search, inputs),
        actionEnum.MofNTerms            => doMofNTerms(search, inputs),
        actionEnum.RankMergeTerms       => doRankMergeTerms(search, inputs),
        actionEnum.RollupByDocument     => doRollupByDocument(search, allInputs[search.inputs[1].stage]),       // more efficient than way normalize is handled, but want to test both varieties
        actionEnum.NormalizeMatch       => doNormalizeMatch(search, inputs),
        actionEnum.Phrase1To5And        => doPhrase1To5And(search, inputs),
        actionEnum.GlobalAtLeast        => doGlobalAtLeast(search, inputs),
        actionEnum.ContainedAtLeast     => doContainedAtLeast(search, inputs),
        actionEnum.TagContainsTerm      => doTagContainsTerm(search, inputs),
        actionEnum.TagContainsSearch    => doTagContainsSearch(search, inputs),
        actionEnum.TagNotContainsTerm   => doTagNotContainsTerm(search, inputs),
        actionEnum.SameContainer        => doSameContainer(search, inputs),
        actionEnum.NotSameContainer     => doNotSameContainer(search, inputs),
        actionEnum.MofNContainer        => doMofNContainer(search, inputs),
//      actionEnum.RankContainer        => doRankContainer(search, inputs),

        actionEnum.AndJoinTerms         => doAndJoinTerms(search, inputs),
        actionEnum.AndNotJoinTerms      => doAndNotJoinTerms(search, inputs),
        actionEnum.MofNJoinTerms        => doMofNJoinTerms(search, inputs),
        actionEnum.RankJoinTerms        => doRankJoinTerms(search, inputs),
        actionEnum.ProximityMergeAnd    => doProximityMergeAnd(search, inputs),
        actionEnum.RollupContainer      => doRollupContainer(search, inputs),
        actionEnum.OverlapProximityAnd  => doOverlapProximityAnd(search, inputs),
        actionEnum.PositionFilter       => doPositionFilter(search, inputs),
        actionEnum.ChooseRange          => doChooseRange(search, inputs),
        actionEnum.ButNotTerms          => doButNotTerms(search, inputs),
        actionEnum.ButNotJoinTerms      => doButNotJoinTerms(search, inputs),

        dataset([], matchRecord));

    //check that outputs from every stage are sorted as required.
    sortedResult := sorted(result, doc, segment, wpos, assert);
    return sortedResult;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Code to actually execute the query:

convertToUserOutput(dataset(matchRecord) results) := function

    simpleUserOutputRecord createUserOutput(matchRecord l) := transform
            self.source := TS_docid2source(l.doc);
            self.subDoc := TS_docid2doc(l.doc);
            self.words := l.children;
            SELF := l;
        END;

    return project(results, createUserOutput(left));
end;

ExecuteQuery(searchDataset queryDefinition, dataset(matchRecord) initialResults = dataset([], matchRecord)) := function

#if (useLocal=true)
    executionPlan := thisnode(global(queryDefinition, opt, few));           // Store globally for efficient access
    results := ALLNODES(LOCAL(graph(initialResults, count(executionPlan), processStage(executionPlan[NOBOUNDCHECK COUNTER], rowset(left)), parallel)));
#else
    executionPlan := global(queryDefinition, opt, few);         // Store globally for efficient access
    results := graph(initialResults, count(executionPlan), processStage(executionPlan[NOBOUNDCHECK COUNTER], rowset(left)), parallel);
#end
    userOutput := convertToUserOutput(results);

    return userOutput;
end;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

// A simplified query language
parseQuery(string queryText) := function

searchParseRecord :=
            RECORD(searchRecord)
unsigned        numInputs;
            END;

productionRecord  :=
            record
unsigned        termCount;
dataset(searchParseRecord) actions{maxcount(MaxActions)};
            end;

unknownTerm := (termType)-1;

PRULE := rule type (productionRecord);
ARULE := rule type (searchParseRecord);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

pattern ws := [' ','\t'];

token number    := pattern('-?[0-9]+');
//pattern wordpat   := pattern('[A-Za-z0-9]+');
pattern wordpat := pattern('[A-Za-z][A-Za-z0-9]*');
pattern quotechar   := '"';
token quotedword := quotechar wordpat quotechar;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

searchParseRecord setCapsFlags(wordFlags mask, wordFlags value, searchParseRecord l) := TRANSFORM
    SELF.wordFlagMask := mask;
    SELF.wordFlagCompare := value;
    SELF := l;
END;


PRULE forwardExpr := use(productionRecord, 'ExpressionRule');

ARULE term0
    := quotedword                               transform(searchParseRecord,
                                                    SELF.action := actionEnum.ReadWord;
                                                    SELF.word := StringLib.StringToLowerCase($1[2..length($1)-1]);
                                                    SELF := []
                                                )
    ;

ARULE capsTerm0
    := term0
    | 'CAPS' '(' term0 ')'                      setCapsFlags(wordFlags.hasUpper, wordFlags.hasUpper, $3)
    | 'NOCAPS' '(' term0 ')'                    setCapsFlags(wordFlags.hasUpper, 0, $3)
    | 'ALLCAPS' '(' term0 ')'                   setCapsFlags(wordFlags.hasUpper+wordFlags.hasLower, wordFlags.hasUpper, $3)
    ;

ARULE term0List
    := term0                                    transform(searchParseRecord,
                                                    SELF.action := actionEnum.ReadWordSet;
                                                    SELF.words := dataset(row(transform(wordRecord, self.word := $1.word)));
                                                    SELF.word := '';
                                                    SELF := $1;
                                                )
    | SELF ',' term0                            transform(searchParseRecord,
                                                    SELF.words := $1.words + dataset(row(transform(wordRecord, self.word := $3.word)));
                                                    SELF := $1;
                                                )
    ;

ARULE capsTerm0List
    := term0List
    | 'CAPS' '(' term0List ')'                  setCapsFlags(wordFlags.hasUpper, wordFlags.hasUpper, $3)
    | 'NOCAPS' '(' term0List ')'                setCapsFlags(wordFlags.hasUpper, 0, $3)
    | 'ALLCAPS' '(' term0List ')'               setCapsFlags(wordFlags.hasUpper+wordFlags.hasLower, wordFlags.hasUpper, $3)
    ;

PRULE termList
    := forwardExpr                              transform(productionRecord, self.termCount := 1; self.actions := $1.actions)
    | SELF ',' forwardExpr                      transform(productionRecord, self.termCount := $1.termCount + 1; self.actions := $1.actions + $3.actions)
    ;

PRULE term1
    := capsTerm0                                transform(productionRecord, self.termCount := 1; self.actions := dataset($1))
    | 'SET' '(' capsTerm0List ')'               transform(productionRecord, self.termCount := 1; self.actions := dataset($3))
    | '(' forwardExpr ')'
    | 'AND' '(' termList ')'                    transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndTerms;
//                                                          self.numInputs := count($3.actions) - sum($3.actions, numInputs);
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'ANDNOT' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotTerms;
                                                            self.numInputs := 2;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'BUTNOT' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ButNotTerms;
                                                            self.numInputs := 2;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'BUTNOTJOIN' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ButNotJoinTerms;
                                                            self.numInputs := 2;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'RANK' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RankMergeTerms;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'MOFN' '(' number ',' termList ')'        transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNTerms;
                                                            self.numInputs := $5.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := $5.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFN' '(' number ',' number ',' termList ')'     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $7.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNTerms;
                                                            self.numInputs := $7.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := (integer)$5;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'OR' '(' termList ')'                     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.OrTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'PHRASE' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.PhraseAnd;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'PHRASE1TO5' '(' termList ')'             transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.Phrase1To5And;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'PROXIMITY' '(' forwardExpr ',' forwardExpr ',' number ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := (integer)$7;
                                                            self.maxDistanceRightAfterLeft := (integer)$9;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'OVERLAP' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.OverlapProximityAnd;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'PRE' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := -1;
                                                            self.maxDistanceRightAfterLeft := MaxWordsInDocument;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'AFT' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := MaxWordsInDocument;
                                                            self.maxDistanceRightAfterLeft := -1;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'PROXMERGE' '(' forwardExpr ',' forwardExpr ',' number ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityMergeAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := (integer)$7;
                                                            self.maxDistanceRightAfterLeft := (integer)$9;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'ANDJOIN' '(' termList ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndJoinTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'ANDNOTJOIN' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotJoinTerms;
                                                            self.numInputs := 2;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFNJOIN' '(' number ',' termList ')'        transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNJoinTerms;
                                                            self.numInputs := $5.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := $5.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFNJOIN' '(' number ',' number ',' termList ')'     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $7.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNJoinTerms;
                                                            self.numInputs := $7.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := (integer)$5;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'RANKJOIN' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RankJoinTerms;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'ROLLAND' '(' termList ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    ) + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RollupByDocument;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'NORM' '(' forwardExpr ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.NormalizeMatch;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'ATLEAST' '(' number ',' forwardExpr ')'  transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.GlobalAtLeast;
                                                            self.minMatches := (integer)$3;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'IN' '(' wordpat ',' forwardExpr ')'      transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.TagContainsSearch;
                                                            self.word := StringLib.StringToLowerCase($3);
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'NOTIN' '(' wordpat ',' forwardExpr ')'   transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.TagNotContainsTerm;
                                                            self.word := StringLib.StringToLowerCase($3);
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'SAME' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.SameContainer;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'P' '(' forwardExpr ')'                   transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.TagContainsSearch;
                                                            self.word := 'p';
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'S' '(' forwardExpr ')'                   transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.TagContainsSearch;
                                                            self.word := 's';
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'AT' '(' forwardExpr ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.PositionFilter;
                                                            self.seekWpos := (integer)$5;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    //Useful for testing leaks on early termination
    | 'FIRST' '(' forwardExpr ',' number ')'    transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ChooseRange;
                                                            self.minMatches := 1;
                                                            self.maxMatches := (integer)$5;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'RANGE' '(' forwardExpr ',' number ',' number ')' transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ChooseRange;
                                                            self.minMatches := (integer)$5;
                                                            self.maxMatches := (integer)$7;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    //Internal - purely for testing the underlying functionality
    | '_ATLEASTIN_' '(' number ',' forwardExpr ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ContainedAtLeast;
                                                            self.minMatches := (integer)$3;
                                                            self.numInputs := 1;
                                                            self.termsToProcess := dataset([createTerm((integer)$7)]);
                                                            self := [];
                                                        )
                                                    )
                                                )
    ;



PRULE expr
    := term1                                    : define ('ExpressionRule')
    ;

infile := dataset(row(transform({ string line{maxlength(1023)} }, self.line := queryText)));

resultsRecord := record
dataset(searchParseRecord) actions{maxcount(MaxActions)};
        end;


resultsRecord extractResults(dataset(searchParseRecord) actions) :=
        TRANSFORM
            SELF.actions := actions;
        END;

p1 := PARSE(infile,line,expr,extractResults($1.actions),first,whole,skip(ws),nocase,parse);

pnorm := normalize(p1, left.actions, transform(right));

//Now need to associate sequence numbers, and correctly set them up.
stageStackRecord := record
    stageType prevStage;
    dataset(stageRecord) stageStack{maxcount(MaxActions)};
end;

nullStack := row(transform(stageStackRecord, self := []));

assignStages(searchParseRecord l, stageStackRecord r) := module

    shared stageType thisStage := r.prevStage + 1;
    shared stageType maxStage := count(r.stageStack);
    shared stageType minStage := maxStage+1-l.numInputs;
    shared thisInputs := r.stageStack[minStage..maxStage];

    export searchParseRecord nextRow := transform
        self.stage := thisStage;
        self.term := thisStage;
        self.inputs := thisInputs;
        self := l;
    end;

    export stageStackRecord nextStack := transform
        self.prevStage := thisStage;
        self.stageStack := r.stageStack[1..maxStage-l.numInputs] + row(createStage(thisStage));
    end;
end;


sequenced := process(pnorm, nullStack, assignStages(left, right).nextRow, assignStages(left, right).nextStack);
return project(sequenced, transform(searchRecord, self := left));

end;

//Calculate the maximum number of words in phrase each operator could have as it's children (for use in proximity)
//easier to process since the graph is stored in reverse polish order
doCalculateMaxWip(searchDataset input) := function

    stageStackRecord := record
        dataset(wipRecord) wipStack{maxcount(MaxActions)};
    end;

    nullStack := row(transform(stageStackRecord, self := []));

    assignStageWip(searchRecord l, stageStackRecord r) := module

        shared numInputs := count(l.inputs);
        shared stageType maxStage := count(r.wipStack);
        shared stageType minStage := maxStage+1-numInputs;

        shared maxLeftWip := r.wipStack[minStage].wip;
        shared maxRightWip := IF(numInputs > 1, r.wipStack[maxStage].wip, 0);
        shared maxChildWip := max(r.wipStack[minStage..maxStage], wip);
        shared sumMaxChildWip := sum(r.wipStack[minStage..maxStage], wip);

        shared thisMaxWip := case(l.action,
                actionEnum.ReadWord=>MaxWipWordOrAlias,
                actionEnum.AndTerms=>maxChildWip,
                actionEnum.OrTerms=>maxChildWip,
                actionEnum.AndNotTerms=>maxLeftWip,
                actionEnum.ButNotTerms=>maxLeftWip,
                actionEnum.ButNotJoinTerms=>maxLeftWip,
                actionEnum.PhraseAnd=>sumMaxChildWip,
                actionEnum.Phrase1To5And=>sumMaxChildWip,
                actionEnum.ProximityAnd=>MAX(l.maxDistanceRightBeforeLeft,l.maxDistanceRightAfterLeft,0) + sumMaxChildWip,
                actionEnum.OverlapProximityAnd=>sumMaxChildWip,
                actionEnum.MofNTerms=>maxChildWip,
                actionEnum.TagContainsTerm=>MaxWipTagContents,
                actionEnum.TagContainsSearch=>MaxWipTagContents,
                maxChildWip);


        export searchRecord nextRow := transform
            self.maxWip := thisMaxWip;
            self.maxWipLeft := maxLeftWip;
            self.maxWipRight := maxRightWip;
            self.maxWipChild := maxChildWip;
            self := l;
        end;

        export stageStackRecord nextStack := transform
            self.wipStack := r.wipStack[1..maxStage-numInputs] + row(transform(wipRecord, self.wip := thisMaxWip;));
        end;
    end;

    return process(input, nullStack, assignStageWip(left, right).nextRow, assignStageWip(left, right).nextStack);
end;

renumberRecord := RECORD
    stageType prevStage;
    dataset(stageMapRecord) map{maxcount(maxStages)};
END;
nullRenumber := row(transform(renumberRecord, self := []));

deleteExpandStages(input, expandTransform, result) := MACRO
    #uniquename (renumberStages)
    #uniquename (stagea)
    #uniquename (stageb)

    %renumberStages%(recordof(input) l, renumberRecord r) := module
        shared prevStage := r.prevStage;
        shared nextStage := prevStage + l.numStages;
        export recordof(input) nextRow := transform
            SELF.stage := prevStage + 1;
            SELF.inputs := project(l.inputs, createStage(r.map(from = left.stage)[1].to));
            SELF := l;
        end;
        export renumberRecord nextRight := transform
            SELF.prevStage := nextStage;
            SELF.map := r.map + row(transform(stageMapRecord, self.from := l.stage; self.to := nextStage));
        end;
    end;

    %stagea% := process(input, nullRenumber, %renumberStages%(left, right).nextRow, %renumberStages%(left, right).nextRight);
    %stageb% := %stagea%(numStages != 0);
    result := normalize(%stageb%, left.numStages, expandTransform(LEFT, COUNTER))
ENDMACRO;



// 1) IN(IN(ATLEAST(n, x))  -> IN(ContainedAtLeast(IN(x))
//    or more complicated....
//    IN:X(ATLEAST(2, x) AND ATLEAST(3, y))
//    ->_ATLEAST_(2, [term:x], _ATLEAST_(3, [term:y], IN:X(x AND y)))
//    The contained atleast is swapped with a surrounding IN, and converted to a contained at least
//
//    Algorithm:
//    a) Gather a list of terms that each atleast works on, and annotate each IN with a list of atleasts.
//    b) Invert the list, and tag any atleasts that are going to be moved.
//    c) Resort the list, remove any atleasts being moved, and wrap each IN with each of the atleasts being moved.

transformAtLeast(searchDataset parsed) := function

    atleastRecord := RECORD
        termType atleastTerm;
        matchCountType minMatches;
        dataset(termRecord) terms{maxcount(MaxTerms)};
    END;

    atleastRecord createAtleast(searchRecord l, dataset(termRecord) terms) := transform
        SELF.atleastTerm := l.term;
        SELF.minMatches := l.minMatches;
        SELF.terms := terms;
    END;

    //Project to the structure that allows processing.
    processRecord := RECORD(searchRecord)
        termType numStages;
        dataset(atleastRecord) moved{maxcount(MaxTerms)};
    END;
    stage0 := project(parsed, transform(processRecord, self := left; self := []));

    //Gather a list of inut and output terms, and a list of atleasts that need moving.
    activeRecord := RECORD
        stageType stage;
        dataset(termRecord) outputTerms{maxcount(MaxTerms)};
        dataset(atleastRecord) activeAtleast{maxcount(MaxTerms)};
    END;
    stage1Record := RECORD
        dataset(activeRecord) mapping;
    END;
    doStage1(processRecord l, stage1Record r) := module
        shared inputTerms := r.mapping(stage in set(l.inputs, stage)).outputTerms;
        shared inputAtleast := r.mapping(stage in set(l.inputs, stage)).activeAtleast;
        shared outputTerms := IF(hasSingleRowPerMatch(l.action), dataset([createTerm(l.term)]), inputTerms);
        shared outputAtleast :=
            MAP(l.action = actionEnum.GlobalAtLeast=>inputAtleast + row(createAtleast(l, inputTerms)),
                l.action <> actionEnum.TagContainsSearch=>inputAtleast);

        export processRecord nextRow := transform
            SELF.moved := IF(l.action = actionEnum.TagContainsSearch, inputAtLeast);
#if (INCLUDE_DEBUG_INFO)
            SELF.debug := trim(l.debug) + '[' +
                    (string)COUNT(inputTerms) + ':' +
                    (string)COUNT(inputAtleast) + ':' +
                    (string)COUNT(outputTerms) + ':' +
                    (string)COUNT(outputAtLeast) + ':' +
                    (string)COUNT(IF(l.action = actionEnum.TagContainsSearch, inputAtLeast)) + ':' +
                    ']';
#end
            SELF := l;
        end;
        export stage1Record nextRight := transform
            SELF.mapping := r.mapping + row(transform(activeRecord, self.stage := l.stage; self.outputTerms := outputTerms; self.activeAtleast := outputAtleast));
        end;
    end;
    nullStage1 := row(transform(stage1Record, self := []));
    stage1 := process(stage0, nullStage1, doStage1(left, right).nextRow, doStage1(left, right).nextRight);
    invertedStage1 := sort(stage1, -stage);

    //Now build up a list of stages that are contained within a tag, and if an atleast is within a tag then mark it for removal
    //Build up a list of inputs which need to be wrapped with inEnsure the at least is only tagged onto the outer IN(), and set the minimum matches to 0 for the inner atleast()
    stage2Record := RECORD
        dataset(stageRecord) mapping{maxcount(MaxStages)};
    END;
    doStage2(processRecord l, stage2Record r) := module
        shared removeThisStage := (l.action = actionEnum.GlobalAtLeast and exists(r.mapping(stage = l.stage)));
        shared numStages := IF(removeThisStage,0,1 + count(l.moved));
        export processRecord nextRow := transform
            SELF.numStages := numStages;
            SELF := l;
        end;
        export stage2Record nextRight := transform
            SELF.mapping := r.mapping + IF(l.action = actionEnum.TagContainsSearch or exists(r.mapping(stage = l.stage)), l.inputs);
        end;
    end;
    nullStage2 := row(transform(stage2Record, self := []));
    stage2 := process(invertedStage1, nullStage2, doStage2(left, right).nextRow, doStage2(left, right).nextRight);
    revertedStage2 := sort(stage2, stage);

    //Remove atleast inside container, add them back around the tag, and renumber stages.
    processRecord duplicateAtLeast(processRecord l, unsigned c) := TRANSFORM
        SELF.stage := l.stage + (c - 1);
        SELF.inputs := IF(c>1, dataset([createStage(l.stage + c - 2)]), l.inputs);
        SELF.action := IF(c>1, actionEnum.ContainedAtLeast, l.action);
        SELF.minMatches := IF(c>1, l.moved[c-1].minMatches, l.minMatches);
        SELF.termsToProcess := IF(c>1, l.moved[c-1].terms, l.termsToProcess);
        SELF := l;
    END;
    deleteExpandStages(revertedStage2, duplicateAtLeast, stage3c);

    return project(stage3c, transform(searchRecord, self := left));
end;



// 2) NOT IN:X((A OR B) AND PHRASE(c, D)) -> OR(NOT IN:X(A),NOT IN:X(B)) AND NOT IN:X(PHRASE(C,D))
//
//    NOT IN (A AND B) means same as NOT IN (A) AND NOT IN (B)
//    NOT IN (A OR B) means same as NOT IN (A) OR NOT IN (B)
//
//    Need to move the NOT IN() operator down, so it surrounds items that are guaranteed to generate a single item

transformNotIn(searchDataset input) := function

    //Project to the structure that allows processing.
    processRecord := RECORD(searchRecord)
        termType numStages;
        boolean singleRowPerMatch;
        boolean inputsSingleRowPerMatch;
        wordType newContainer;
        termType newTerm;
    END;
    stage0 := project(input, transform(processRecord, self := left; self := []));

    //Annotate all nodes with whether or not they are single valued.  (So simple OR phrase is noted as single)
    //the atleast id/min of any ATLEASTs they directly contain
    stage1Record := RECORD
        dataset(booleanRecord) isSingleMap;
    END;
    doStage1(processRecord l, stage1Record r) := module
        shared inputsSingleValued := not exists(l.inputs(not r.isSingleMap[l.inputs.stage].value));
        shared isSingleValued := IF(inheritsSingleRowPerMatch(l.action), inputsSingleValued, hasSingleRowPerMatch(l.action));
        export processRecord nextRow := transform
            SELF.singleRowPerMatch := isSingleValued;
            SELF.inputsSingleRowPerMatch := inputsSingleValued;
#if (INCLUDE_DEBUG_INFO)
            SELF.debug := trim(l.debug) + '[' + TF(isSingleValued) + TF(inputsSingleValued) + ']';
#end
            SELF := l;
        end;
        export stage1Record nextRight := transform
            SELF.isSingleMap := r.isSingleMap + row(transform(booleanRecord, self.value := isSingleValued));
        end;
    end;
    nullStage1 := row(transform(stage1Record, self := []));
    stage1 := process(stage0, nullStage1, doStage1(left, right).nextRow, doStage1(left, right).nextRight);
    invertedStage1 := sort(stage1, -stage);

    //Build up a list of inputs which need to be wrapped with inEnsure the at least is only tagged onto the outer IN(), and set the minimum matches to 0 for the inner atleast()
    mapRecord := RECORD
        stageType stage;
        wordType container;
        termType term;
    END;
    stage2Record := RECORD
        dataset(mapRecord) map;
    END;
    doStage2(processRecord l, stage2Record r) := module
        shared newContainer := r.map(stage = l.stage)[1].container;
        shared newTerm := r.map(stage = l.stage)[1].term;
        shared numStages :=
                    MAP(l.singleRowPerMatch and newContainer <> ''=>2,
                        l.action = actionEnum.TagNotContainsTerm and not l.singleRowPerMatch=>0,
                        1);
        export processRecord nextRow := transform
            SELF.newContainer := newContainer;
            SELF.newTerm := newTerm;
            SELF.numStages := numStages;
#if (INCLUDE_DEBUG_INFO)
            SELF.debug := trim(l.debug) + '[' + newContainer + ']';
#end
            SELF := l;
        end;
        export stage2Record nextRight := transform
            SELF.map := r.map + MAP(l.action = actionEnum.TagNotContainsTerm and not l.inputsSingleRowPerMatch=>
                                        PROJECT(l.inputs, transform(mapRecord, SELF.stage := LEFT.stage; SELF.container := l.word; SELF.term := l.term)),
                                    not l.singleRowPerMatch and (newContainer <> '')=>
                                        PROJECT(l.inputs, transform(mapRecord, SELF.stage := LEFT.stage; SELF.container := newContainer; SELF.term := newTerm)));
        end;
    end;
    nullStage2 := row(transform(stage2Record, self := []));
    stage2 := process(invertedStage1, nullStage2, doStage2(left, right).nextRow, doStage2(left, right).nextRight);
    revertedStage2 := sort(stage2, stage);

    //Map the operators within the container, add map the outer TagContainsSearch to a RollupContainer
    processRecord duplicateContainer(processRecord l, unsigned c) := TRANSFORM
        SELF.stage := l.stage + (c - 1);
        SELF.inputs := IF(c=2, dataset([createStage(l.stage)]), l.inputs);
        SELF.action := IF(c=2, actionEnum.TagNotContainsTerm, l.action);
        SELF.word := IF(c=2, l.newContainer, l.word);
        SELF.term := IF(c=2, l.newTerm, l.term);
        SELF := l;
    END;
    deleteExpandStages(revertedStage2, duplicateContainer, stage3c);

    result := project(stage3c, transform(searchRecord, self := left));
    //RETURN IF (exists(input(action = actionEnum.TagContainsSearch)), result, input);
    RETURN result;
end;

// 3) IN:X(OR((A OR B) AND PHRASE(c, D)) -> SAME(OR(IN:X(A),IN:X(B)), IN:X(PHRASE(C,D))
//
//    Need to move the IN() operator down, so it surrounds items that are guaranteed to generate a single item
//    per match, and then convert intervening operators as follows
//    AND->SAMEWORD, OR->OR, ANDNOT-> NOTSAMEWORD, MOFN->MOFNWORD, RANK->RANKWORD
//    the in is moved down to all the operators below that generate a single row per match.
//
//    Note, PHRASE and PROXIMITY create single items, so they will work as expected.
//    (a or b) BUTNOT (c) may have issues

transformIn(searchDataset input) := function

    //Project to the structure that allows processing.
    processRecord := RECORD(searchRecord)
        termType numStages;
        boolean singleRowPerMatch;
        boolean inputsSingleRowPerMatch;
        wordType newContainer;
        termType newTerm;
    END;
    stage0 := project(input, transform(processRecord, self := left; self := []));

    //Annotate all nodes with whether or not they are single valued.  (So simple OR phrase is noted as single)
    //the atleast id/min of any ATLEASTs they directly contain
    stage1Record := RECORD
        dataset(booleanRecord) isSingleMap;
    END;
    doStage1(processRecord l, stage1Record r) := module
        shared inputsSingleValued := not exists(l.inputs(not r.isSingleMap[l.inputs.stage].value));
        shared isSingleValued := IF(inheritsSingleRowPerMatch(l.action), inputsSingleValued, hasSingleRowPerMatch(l.action));
        export processRecord nextRow := transform
            SELF.singleRowPerMatch := isSingleValued;
            SELF.inputsSingleRowPerMatch := inputsSingleValued;
#if (INCLUDE_DEBUG_INFO)
            SELF.debug := trim(l.debug) + '[' + TF(isSingleValued) + TF(inputsSingleValued) + ']';
#end
            SELF := l;
        end;
        export stage1Record nextRight := transform
            SELF.isSingleMap := r.isSingleMap + row(transform(booleanRecord, self.value := isSingleValued));
        end;
    end;
    nullStage1 := row(transform(stage1Record, self := []));
    stage1 := process(stage0, nullStage1, doStage1(left, right).nextRow, doStage1(left, right).nextRight);
    invertedStage1 := sort(stage1, -stage);

    //Build up a list of inputs which need to be wrapped with inEnsure the at least is only tagged onto the outer IN(), and set the minimum matches to 0 for the inner atleast()
    mapRecord := RECORD
        stageType stage;
        wordType container;
        termType term;
    END;
    stage2Record := RECORD
        dataset(mapRecord) map;
    END;
    doStage2(processRecord l, stage2Record r) := module
        shared newContainer := r.map(stage = l.stage)[1].container;
        shared newTerm := r.map(stage = l.stage)[1].term;
        shared numStages := IF(l.singleRowPerMatch and newContainer <> '', 2, 1);
        export processRecord nextRow := transform
            SELF.newContainer := newContainer;
            SELF.newTerm := newTerm;
            SELF.numStages := numStages;
#if (INCLUDE_DEBUG_INFO)
            SELF.debug := trim(l.debug) + '[' + newContainer + ']';
#end
            SELF := l;
        end;
        export stage2Record nextRight := transform
            SELF.map := r.map + MAP(l.action = actionEnum.TagContainsSearch and not l.inputsSingleRowPerMatch=>
                                        PROJECT(l.inputs, transform(mapRecord, SELF.stage := LEFT.stage; SELF.container := l.word; SELF.term := l.term)),
                                    not l.singleRowPerMatch and (newContainer <> '')=>
                                        PROJECT(l.inputs, transform(mapRecord, SELF.stage := LEFT.stage; SELF.container := newContainer; SELF.term := newTerm)));
        end;
    end;
    nullStage2 := row(transform(stage2Record, self := []));
    stage2 := process(invertedStage1, nullStage2, doStage2(left, right).nextRow, doStage2(left, right).nextRight);
    revertedStage2 := sort(stage2, stage);

    //Map the operators within the container, add map the outer TagContainsSearch to a RollupContainer
    processRecord duplicateContainer(processRecord l, unsigned c) := TRANSFORM
        actionEnum mappedAction :=
            CASE(l.action,
                 actionEnum.AndTerms        => IF(l.newContainer <> '', actionEnum.SameContainer, l.action),
                 actionEnum.AndNotTerms     => IF(l.newContainer <> '', actionEnum.NotSameContainer, l.action),
                 actionEnum.MofNTerms       => IF(l.newContainer <> '', actionEnum.MofNContainer, l.action),
                 actionEnum.RankMergeTerms  => IF(l.newContainer <> '', actionEnum.RankContainer, l.action),
                 actionEnum.TagContainsSearch => IF(l.inputsSingleRowPerMatch, actionEnum.TagContainsSearch, actionEnum.rollupContainer),
                 l.action);


        SELF.stage := l.stage + (c - 1);
        SELF.inputs := IF(c=2, dataset([createStage(l.stage)]), l.inputs);
        SELF.action := IF(c=2, actionEnum.TagContainsTerm, mappedAction);
        SELF.word := IF(c=2, l.newContainer, l.word);
        SELF.term := IF(c=2, l.newTerm, l.term);
        SELF := l;
    END;
    deleteExpandStages(revertedStage2, duplicateContainer, stage3c);

    result := project(stage3c, transform(searchRecord, self := left));
    //RETURN IF (exists(input(action = actionEnum.TagContainsSearch)), result, input);
    RETURN result;
end;

queryProcessor(string query) := module//,library('TextSearch',1,0)
export string queryText := query;
export request := parseQuery(query);
processed0 := request;
processed1 := transformAtLeast(processed0);
processed2 := transformNotIn(processed1);
processed3 := transformIn(processed2);
processed4 := doCalculateMaxWip(processed3);
export processed := processed4;
export result := ExecuteQuery(processed);
    end;

inputRecord := { string query{maxlength(2048)}; };

MaxResults := 10000;

processedRecord := record(inputRecord)
searchDataset request{maxcount(MaxActions)}; // you should be able to use a typedef to define a field.
dataset(simpleUserOutputRecord) result{maxcount(MaxResults)};
        end;


processedRecord doBatchExecute(inputRecord l) := transform
    processed := queryProcessor(l.query);
    self.request := processed.processed;
    self.result := processed.result;
    self := l;
end;


doSingleExecute(string queryText) := function
    request := parseQuery(queryText);
    result := ExecuteQuery(request);
    return result;
end;

q1 := dataset([

            'AND("black","sheep")',
            'ANDNOT("black","sheep")',
            'MOFN(2,"black","sheep","white")',
            'MOFN(2,2,"black","sheep","white")',

//Word tests
            '("nonexistant")',
            '("one")',
            'CAPS("one")',
            'NOCAPS("one")',
            'ALLCAPS("one")',
            '"ibm"',                                        // simple word, and an alias

//Or tests
            'OR("nonexistant1", "nonexistant2")',           // neither side matches
            'OR("nonexistant1", "sheep")',                  // RHS matches
            'OR("twinkle", "nonexistant2")',                // LHS matches
            'OR("twinkle", "twinkle")',                     // should dedup
            'OR("sheep", "black")',                         // matches in same document
            'OR("sheep", "twinkle")',                       // matches in different documents
            'OR("one", "sheep", "sheep", "black", "fish")', // matches in different documents
            'OR(OR("one", "sheep"), OR("sheep", "black", "fish"))', // matches in different documents

//And tests
            'AND("nonexistant1", "nonexistant2")',          // neither side matches
            'AND("nonexistant1", "sheep")',                 // RHS matches
            'AND("twinkle", "nonexistant2")',               // LHS matches
            'AND("twinkle", "twinkle")',                    // should dedup
            'AND("sheep", "black")',                        // matches in same document
            'AND("sheep", "twinkle")',                      // matches in different documents
            'AND("in", "a")',                               // high frequencies
            'AND("twinkle", "little", "how", "star")',      // Nary
            'AND(AND("twinkle", "little"), AND("how", "star"))',        // Nested
            'AND(AND("twinkle", "little"), AND("how", "wonder"))',      // Nested

//MORE: Should also test segment restriction....

            'ANDNOT("nonexistant1", "nonexistant2")',       // neither side matches
            'ANDNOT("nonexistant1", "sheep")',              // RHS matches
            'ANDNOT("twinkle", "nonexistant2")',            // LHS matches
            'ANDNOT("twinkle", "twinkle")',                 // should dedup
            'ANDNOT("sheep", "black")',                     // matches in same document
            'ANDNOT("sheep", "twinkle")',                   // matches in different documents
            'ANDNOT("one", OR("sheep", "black", "fish"))',      // matches one, but none of the others

//Phrases
            'PHRASE("nonexistant1", "nonexistant2")',       // words don't occour
            'PHRASE("in", "are")',                          // doesn't occur, but words do
            'PHRASE("baa", "black")',                       // occurs, but
            'PHRASE("x", "y", "x", "x", "y")',              // a partial match, first - doesn't actually make it more complicatied to implement
            'PHRASE("james","arthur","stuart")',            // check that next occurence of stuart takes note of the change of document.
            'PHRASE(OR("black","white"),"sheep")',          // proximity on a non-word input
            'PHRASE("one", "for", OR(PHRASE("the","master"),PHRASE("the","dame"),PHRASE("the","little","boy")))',
                                                            // more complex again

//Testing range
            'PHRASE1to5("humpty","dumpty")',
            'PHRASE1to5("together","again")',

//M of N
            'MOFN(2, "humpty", "horses", "together", "beansprout")',    // m<matches
            'MOFN(3, "humpty", "horses", "together", "beansprout")',    // m=matches
            'MOFN(4, "humpty", "horses", "together", "beansprout")',    // m>matches
            'MOFN(2,2, "humpty", "horses", "together", "beansprout")',  // too many matches
            'MOFN(2, "nonexistant", "little", "bo")',                   // first input fails to match any
            'MOFN(2, "little", "bo", "nonexistant")',                   // lose an input while finising candidates
            'MOFN(2, "one", "two", "three", "four", "five")',
            'MOFN(2, "nonexistant", "two", "three", "four", "five")',
            'MOFN(2, "one", "nonexistant", "three", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "three", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "nonexistant3", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "nonexistant3", "nonexistant4", "five")',
            'MOFN(2, PHRASE("little","bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases
            'MOFN(2, PHRASE("Little","Bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases - capital letters don't match
            'MOFN(2, OR("little","big"), OR("king", "queen"), OR("star", "sheep", "twinkle"))',

//Proximity
            'PROXIMITY("nonexistant1", "nonexistant2", -1, 1)',
            'PROXIMITY("black", "nonexistant2", -1, 1)',
            'PROXIMITY("nonexistant1", "sheep", -1, 1)',

//Adjacent checks
            'PROXIMITY("ship", "sank", 0, 0)',                      // either order but adjacent
            'NORM(PROXIMITY("ship", "sank", 0, 0))',
            'PROXIMITY("ship", "sank", -1, 0)',                     // must follow
            'PROXIMITY("ship", "sank", 0, -1)',                     // must preceed
            'PROXIMITY("sank", "ship", 0, 0)',                      // either order but adjacent
            'PROXIMITY("sank", "ship", -1, 0)',                     // must follow
            'PROXIMITY("sank", "ship", 0, -1)',                     // must preceed

//Within a distance of 1
            'PROXIMITY("ship", "sank", 1, 1)',                      // either order but only 1 intervening word
            'PROXIMITY("ship", "sank", -1, 1)',                     // must follow
            'PROXIMITY("ship", "sank", 1, -1)',                     // must preceed
            'PROXIMITY("sank", "ship", 1, 1)',                      // either order but only 1 intervening word
            'PROXIMITY("sank", "ship", -1, 1)',                     // must follow
            'PROXIMITY("sank", "ship", 1, -1)',                     // must preceed

            'PROXIMITY("ship", "sank", 0, 2)',                      // asymetric range

//Within a distance of 2
            'PROXIMITY("ship", "ship", 2, 2)',                      // either order but only 2 intervening word, no dups
                                                                    // *** currently fails because of lack of duplication in lowest merger
            'PROXIMITY("zx", "zx", 5, 5)',                          // "zx (za) zx", "zx (za zx zb zc zd) zx" and "zx (zb zc zd zx)"
            'PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 1, 1)',   // "zx (za) zx (zb zc zd) zx" - obtained two different ways.
            'NORM(PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 1, 1))', // as above, but normalized
            'PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 0, 0)',   // "zx (za) zx (zb zc zd) zx" - can obly be obtained from first
                                                                    // you could imagine -ve left and right to mean within - would need -1,0 in stepping, and appropriate hard condition.

            'PROXIMITY("ibm", "business", 2, 2)',                   // alias doesn't allow matching within itself.
            'PROXIMITY("ibm", "business", 3, 3)',                   // alias should match now with other word
            'PROXIMITY("ibm", "ibm", 0, 0)',                        // aliases and non aliases cause fun.

//More combinations of operators
            'AND(OR("twinkle", "black"), OR("sheep", "wonder"))',
            'OR(AND("twinkle", "sheep"), AND("star", "black"))',
            'OR(AND("twinkle", "star"), AND("sheep", "black"))',
            'AND(SET("twinkle", "black"), SET("sheep", "wonder"))',

//Silly queries
            'OR("star","star","star","star","star")',
            'AND("star","star","star","star","star")',
            'MOFN(4,"star","star","star","star","star")',


//Other operators
            'PRE("twinkle", "twinkle")',
            'PRE(PHRASE("twinkle", "twinkle"), PHRASE("little","star"))',
            'PRE(PHRASE("little","star"), PHRASE("twinkle", "twinkle"))',
            'PRE(PROXIMITY("twinkle","twinkle", 3, 3), PROXIMITY("little", "star", 2, 2))',
            'AFT("twinkle", "twinkle")',
            'AFT(PHRASE("little","star"), PHRASE("twinkle", "twinkle"))',
            'AFT(PHRASE("twinkle", "twinkle"), PHRASE("little","star"))',
            'AFT(PROXIMITY("twinkle","twinkle", 3, 3), PROXIMITY("little", "star", 2, 2))',

// Left outer joins for ranking.
            'RANK("sheep", OR("peep", "baa"))',
            'RANK("three", OR("bags", "full"))',
            'RANK("three", OR("one", "bags"))',


//Non standard variants - AND, generating a single record for the match.  Actually for each cross product as it is currently (and logically) implemented
            'ANDJOIN("nonexistant1", "nonexistant2")',          // neither side matches
            'ANDJOIN("nonexistant1", "sheep")',                 // RHS matches
            'ANDJOIN("twinkle", "nonexistant2")',               // LHS matches
            'ANDJOIN("twinkle", "twinkle")',                    // should dedup
            'ANDJOIN("sheep", "black")',                        // matches in same document
            'ANDJOIN("sheep", "twinkle")',                      // matches in different documents
            'ANDJOIN("in", "a")',                               // high frequencies
            'ANDJOIN("twinkle", "little", "how", "star")',      // Nary

            'ANDNOTJOIN("nonexistant1", "nonexistant2")',       // neither side matches
            'ANDNOTJOIN("nonexistant1", "sheep")',              // RHS matches
            'ANDNOTJOIN("twinkle", "nonexistant2")',            // LHS matches
            'ANDNOTJOIN("twinkle", "twinkle")',                 // should dedup
            'ANDNOTJOIN("sheep", "black")',                     // matches in same document
            'ANDNOTJOIN("sheep", "twinkle")',                   // matches in different documents
            'ANDNOTJOIN("one", OR("sheep", "black", "fish"))',  // matches one, but none of the others

            'MOFNJOIN(2, "humpty", "horses", "together", "beansprout")',    // m<matches
            'MOFNJOIN(3, "humpty", "horses", "together", "beansprout")',    // m=matches
            'MOFNJOIN(4, "humpty", "horses", "together", "beansprout")',    // m>matches
            'MOFNJOIN(2,2, "humpty", "horses", "together", "beansprout")',  // too many matches
            'MOFNJOIN(2, "nonexistant", "little", "bo")',                   // first input fails to match any
            'MOFNJOIN(2, "little", "bo", "nonexistant")',                   // lose an input while finising candidates
            'MOFNJOIN(2, "one", "two", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant", "two", "three", "four", "five")',
            'MOFNJOIN(2, "one", "nonexistant", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "nonexistant3", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "nonexistant3", "nonexistant4", "five")',
            'MOFNJOIN(2, PHRASE("little","bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases
            'MOFNJOIN(2, PHRASE("Little","Bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases - capital letters don't match
            'MOFNJOIN(2, OR("little","big"), OR("king", "queen"), OR("star", "sheep", "twinkle"))',

            'RANKJOIN("SHEEP", "BLACK")',
            'RANKJOIN("sheep", OR("peep", "baa"))',
            'RANKJOIN("three", OR("bags", "full"))',
            'RANKJOIN("three", OR("one", "bags"))',


//ROLLAND - does AND, followed by a rollup by doc.  Should also check that smart stepping still works through the grouped rollup
            'ROLLAND("nonexistant1", "nonexistant2")',          // neither side matches
            'ROLLAND("nonexistant1", "sheep")',                 // RHS matches
            'ROLLAND("twinkle", "nonexistant2")',               // LHS matches
            'ROLLAND("twinkle", "twinkle")',                    // should dedup
            'ROLLAND("sheep", "black")',                        // matches in same document
            'ROLLAND("sheep", "twinkle")',                      // matches in different documents
            'ROLLAND("in", "a")',                               // high frequencies
            'ROLLAND("twinkle", "little", "how", "star")',      // Nary
            'AND(ROLLAND("twinkle", "little"), ROLLAND("how", "star"))',        // Nary

//Same tests as proximity above, but not calling a transform - merging instead
            'PROXMERGE("ship", "sank", 0, 0)',                      // either order but adjacent
            'PROXMERGE("ship", "sank", -1, 0)',                     // must follow
            'PROXMERGE("ship", "sank", 0, -1)',                     // must preceed
            'PROXMERGE("sank", "ship", 0, 0)',                      // either order but adjacent
            'PROXMERGE("sank", "ship", -1, 0)',                     // must follow
            'PROXMERGE("sank", "ship", 0, -1)',                     // must preceed
            'PROXMERGE("ship", "sank", 1, 1)',                      // either order but only 1 intervening word
            'PROXMERGE("ship", "sank", -1, 1)',                     // must follow
            'PROXMERGE("ship", "sank", 1, -1)',                     // must preceed
            'PROXMERGE("sank", "ship", 1, 1)',                      // either order but only 1 intervening word
            'PROXMERGE("sank", "ship", -1, 1)',                     // must follow
            'PROXMERGE("sank", "ship", 1, -1)',                     // must preceed
            'PROXMERGE("ship", "sank", 0, 2)',                      // asymetric range

//SET should be equivalent to OR
            'SET("nonexistant1", "nonexistant2")',          // neither side matches
            'SET("nonexistant1", "sheep")',                 // RHS matches
            'SET("twinkle", "nonexistant2")',               // LHS matches
            'SET("twinkle", "twinkle")',                    // should dedup
            'SET("sheep", "black")',                        // matches in same document
            'SET("sheep", "twinkle")',                      // matches in different documents
            'SET("one", "sheep", "sheep", "black", "fish")',    // matches in different documents
            'OR(SET("one", "sheep"), SET("sheep", "black", "fish"))',   // matches in different documents

//Testing range
            'PHRASE1to5(PHRASE1to5("what","you"),"are")',
            'PHRASE1to5("what", PHRASE1to5("you","are"))',
            'PHRASE1to5(PHRASE1to5("open","source"),"software")',
            'PHRASE1to5("open", PHRASE1to5("source","software"))',

//Atleast
            'ATLEAST(2, "twinkle")',                                // would something like UNIQUEAND("twinkle", "twinkle") be more efficient???
            'ATLEAST(4, "twinkle")',
            'ATLEAST(5, "twinkle")',
            'ATLEAST(5, AND("twinkle","star"))',
            'AND(ATLEAST(4, "twinkle"),"star")',                    // make sure this still smart steps!
            'AND(ATLEAST(5, "twinkle"),"star")',
            'ATLEAST(1, PHRASE("humpty","dumpty"))',
            'ATLEAST(2, PHRASE("humpty","dumpty"))',
            'ATLEAST(3, PHRASE("humpty","dumpty"))',

            '"little"',
            'IN(name, "little")',
            'NOTIN(name, "little")',
            'IN(suitcase, AND("sock", "shirt"))',
            'IN(suitcase, AND("sock", "dress"))',
            'IN(suitcase, AND("shirt", "dress"))',                  //no, different suitcases..
            'IN(suitcase, OR("cat", "dog"))',                       //no - wrong container
            'IN(box, OR("cat", "dog"))',                            //yes
            'IN(box, IN(suitcase, "shirt"))',
            'IN(suitcase, IN(box, "shirt"))',                       // no other elements in the suitcase, so not valid
            'IN(box, AND(IN(suitcase, "shirt"), "car"))',
            'IN(box, AND(IN(suitcase, "shirt"), "lego"))',          // no, lego isn't in the box...
            'IN(box, MOFN(2, "car", "train", "glue"))',             // really nasty - need to modify the m of n to add position equality!
            'IN(box, MOFN(2, "car", "glue", "train"))',             // and check works in all positions.
            'IN(box, MOFN(2, "glue", "car", "train"))',             //   " ditto "
            'IN(box, MOFN(3, "car", "train", "glue"))',
            'NOTIN(box, AND("computer", "lego"))',
            'NOTIN(box, AND("train", "lego"))',
            'IN(suitcase, PROXIMITY("trouser", "sock", 1, 2))',     // ok.
            'IN(suitcase, PROXIMITY("trouser", "train", 1, 2))',    // no, close enough, but not both in the suitcase
            'IN(suitcase, PROXIMITY("trouser", "dress", 6, 6))',    // no, close enough, but not both in the same suitcase
            'PROXIMITY(IN(suitcase, "trouser"), IN(suitcase, "dress"), 6, 6)',  // yes - testing the proximity of the suitcases, not the contents.

            'IN(S, AND("fish", "alive"))',                          // <s> is the sentence container
            'S(AND("fish", "alive"))',                              // pseudonym for within same sentence -
            'S(AND("fish", "finger"))',                             //
            'S(AND("sheep", "wagging"))',
            'P(AND("sheep", "wagging"))',                           // same paragraph...
            'AND(IN(socks, "fox"),IN(socks, "knox"))',
            'AND(IN(box, "fox"),IN(box, "knox"))',
            'AND(IN(box, IN(socks, "fox")),IN(box, "knox"))',
            'AND(IN(socks, IN(box, "fox")),IN(box, "knox"))',           // yes - because no extra elements in the box.
            'S(PHRASE("black", "sheep"))',
            'IN(name, PHRASE("little", "bo", "peep"))',
            'IN(name, PHRASE("little", "bo", "peep", "has"))',

            'IN(range1, IN(range2, "seven"))',                      // only match 5.3
            'SAME(IN(range1, "seven"), IN(range2, "seven"))',       // only match 5.3
            'OVERLAP(IN(range1, "five"), IN(range2, "ten"))',       // overlapping, match 5.4
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 0, 0)',   // adjacent match 5.4, 5.5
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 1, 1)',   // proximity match 5.4, 5.5
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 2, 2)',   // adjacent match 5.4, 5.5, 5.6

            'ATLEAST(2, IN(suitcase, "sock"))',                     // at least two suitcases containing a sock.
            'ATLEAST(3, IN(suitcase, "sock"))',                     // no

            'IN(box, "train")',                                     // should be 4 matches (since inside nested boxes)

            'IN(suitcase, ATLEAST(1, "sock"))',                     // suitcases containing at least one sock.   (should really optimize away)
            'IN(suitcase, ATLEAST(2, "sock"))',                     // at least two suit cases containing a sock.
            'IN(suitcase, ATLEAST(3, "sock"))',
            'IN(suitcase, ATLEAST(3, OR("sock", "dress")))',        //no
            'IN(suitcase, ATLEAST(3, SET("sock", "dress")))',       //no
            'IN(suitcase, ATLEAST(3, OR("sock", "jacket")))',       //yes...
            'IN(suitcase, ATLEAST(3, SET("sock", "jacket")))',      //yes...
            'IN(box, IN(suitcase, ATLEAST(2, "sock")))',            //yes - box, with one match
            'IN(box, IN(suitcase, ATLEAST(3, "sock")))',            //no -
            'IN(box, ATLEAST(2, IN(suitcase, "sock")))',            //yes -
            'IN(box, ATLEAST(3, IN(suitcase, "sock")))',            //no -
            'IN(box, ATLEAST(2, IN(suitcase, ATLEAST(2, "sock"))))',            //no...
            'IN(box, AND(ATLEAST(2, "train"), ATLEAST(2, "sock")))',    // yes
            'IN(box, AND(ATLEAST(3, "train"), ATLEAST(2, "sock")))',    // no
            'IN(suitcase, AND(ATLEAST(2, "sock"), ATLEAST(2, OR("tights", "dress"))))', // no - different suitcases.
            'IN(suitcase, ATLEAST(2, "sock"))', // yes
            'IN(suitcase, ATLEAST(2, OR("tights", "dress")))',  // yes

//The following example fails - not quite sure how to fix it.
//          'IN(suitcase, OR(ATLEAST(2, "sock"), ATLEAST(2, OR("tights", "dress"))))',  // yes
            'IN(suitcase, ATLEAST(4, AND(ATLEAST(2, "sock"), OR("shirt", "trouser"))))',    // yes - nested atleasts...
            'IN(suitcase, ATLEAST(5, AND(ATLEAST(2, "sock"), OR("shirt", "trouser"))))',    // no

            '_ATLEASTIN_(1, IN(suitcase, "sock"), 1)',                      // suitcases containing at least one sock.   (should really optimize away)
            '_ATLEASTIN_(2, IN(suitcase, "sock"), 1)',                      // at least two suit cases containing a sock.
            '_ATLEASTIN_(3, IN(suitcase, "sock"), 1)',

            'S(ANDNOT("fish", "alive"))',                               // pseudonym for within same sentence -
            'S(ANDNOT("fish", "finger"))',                              //

            'AT("the", 2)',                                             // occurences of 'the' at position 2
            'AT("the", 18)',
            'AT("is", 17)',
            'AND(AT("the", 18),AT("is",17))',

            'AND("gch01", "gch02", "gch04")',
            'AND("gch01", "gch02", "gch10")',

            'AND(SET("and","a"), SET("the", "one"), PHRASE("for","the","dame"))',
            'AND(CAPS("sheep"), "spotted")',
            'AND(CAPS("sheep"), NOCAPS("spotted"))',
            'AND(SET(CAPS("sheep","little")), SET(CAPS("Up","go")))',
            'AND(SET(CAPS("sheep","little")), SET(NOCAPS("Up","go")))',
            'AND(OR(CAPS("sheep"),CAPS("Little")), OR(CAPS("Up"),NOCAPS("go")))',

            'ANDNOT(AND("black","sheep"), "family")',
            'ANDNOT(AND("little","and"), "jack")',

            'BUTNOT("little", PHRASE("little", "star"))',
            'BUTNOTJOIN("little", PHRASE("little", "star"))',
            'BUTNOT("black", PHRASE("black", OR("spotted", "sheep")))',
            'BUTNOTJOIN("black", PHRASE("black", OR("spotted", "sheep")))',

//MORE:
// STEPPED flag on merge to give an error if input doesn't support stepping.
// What about the duplicates that can come out of the proximity operators?
// where the next on the rhs is at a compatible position, but in a different document
// What about inverse of proximity x not w/n y
// Can inverse proximity be used for sentance/paragraph.  Can we combine them so short circuited before temporaries created.
//MORE: What other boundary conditions can we think of.

                ''
            ], inputRecord);

p := project(q1, doBatchExecute(LEFT));
output(p);
