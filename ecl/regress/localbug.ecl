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

#option ('targetClusterType', 'roxie');
import lib_stringlib,std.system.thorlib;
prefix := 'hthor';
useLayoutTrans := false;
useLocal := true;
usePayload := false;
useVarIndex := false;
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


DG_FileOut           := '~REGRESS::' + prefix + '::DG_';
DG_ParentFileOut     := '~REGRESS::' + prefix + '::DG_Parent.d00';
DG_ChildFileOut      := '~REGRESS::' + prefix + '::DG_Child.d00';
DG_GrandChildFileOut := '~REGRESS::' + prefix + '::DG_GrandChild.d00';
DG_FetchFileName     := '~REGRESS::' + prefix + '::DG_FetchFile';
DG_FetchIndex1Name   := '~REGRESS::' + prefix + '::DG_FetchIndex1';
DG_FetchIndex2Name   := '~REGRESS::' + prefix + '::DG_FetchIndex2';
DG_FetchIndexDiffName:= '~REGRESS::' + prefix + '::DG_FetchIndexDiff';
DG_MemFileName       := '~REGRESS::' + prefix + '::DG_MemFile';

//record structures
DG_FetchRecord := RECORD
  INTEGER8 sequence;
  STRING2  State;
  STRING20 City;
  STRING25 Lname;
  STRING15 Fname;
END;

DG_FetchFile   := DATASET(DG_FetchFileName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT);
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
sqNamePrefix := '~REGRESS::' + prefix + '::';
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
TS_MaxProximity         := 10;
TS_MaxWildcard          := 1000;
TS_MaxMatchPerDocument  := 1000;
TS_MaxFilenameLength        := 255;
TS_MaxActions           := 255;

TS_sourceType       := unsigned2;
TS_wordCountType        := unsigned8;
TS_segmentType      := unsigned1;
TS_wordPosType      := unsigned8;
TS_docPosType       := unsigned8;
TS_documentId       := unsigned8;
TS_termType         := unsigned1;
TS_distanceType     := integer8;
TS_indexWipType     := unsigned1;
TS_wipType          := unsigned8;
TS_stageType            := unsigned1;
TS_dateType         := unsigned8;

TS_sourceType TS_docid2source(TS_documentId x) := (x >> 48);
TS_documentId TS_docid2doc(TS_documentId x) := (x & 0xFFFFFFFFFFFF);
TS_documentId TS_createDocId(TS_sourceType source, TS_documentId doc) := (TS_documentId)(((unsigned8)source << 48) | doc);
boolean      TS_docMatchesSource(TS_documentId docid, TS_sourceType source) := (docid between TS_createDocId(source,0) and (TS_documentId)(TS_createDocId(source+1,0)-1));

TS_wordType := string20;
TS_wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);

TS_wordIdType       := unsigned4;

TS_NameWordIndex        := '~REGRESS::' + prefix + '::TS_wordIndex';

TS_wordIndex        := index({ TS_wordType word, TS_documentId doc, TS_segmentType segment, TS_wordPosType wpos, TS_indexWipType wip } , { TS_wordFlags flags, TS_wordType original, TS_docPosType dpos}, TS_NameWordIndex);

TS_wordIndexRecord := recordof(TS_wordIndex);


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

#option ('checkAsserts',true);
import lib_stringLib;

MaxTerms            := TS_MaxTerms;
MaxProximity        := TS_MaxProximity;
MaxWildcard     := TS_MaxWildcard;
MaxMatchPerDocument := TS_MaxMatchPerDocument;
MaxFilenameLength := TS_MaxFilenameLength;
MaxActions       := TS_MaxActions;

sourceType      := TS_sourceType;
wordCountType   := TS_wordCountType;
segmentType     := TS_segmentType;
wordPosType     := TS_wordPosType;
docPosType      := TS_docPosType;
documentId      := TS_documentId;
termType            := TS_termType;
distanceType        := TS_distanceType;
stageType       := TS_stageType;
dateType            := TS_dateType;
wordType            := TS_wordType;
wordFlags       := TS_wordFlags;
wordIdType      := TS_wordIdType;

wordIndex := TS_wordIndex;

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
wordIndexRecord := TS_wordIndexRecord;

MaxWipIndexEntry := 4;
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

//The following aren't very sensible as far as text searching goes, but are here to test the underlying functionality
    AndJoinTerms,       // join on non-proximity
    AndNotJoinTerms,    //
    MofNJoinTerms,      // minMatches, maxMatches
    RankJoinTerms,      // left outer join
    ProximityMergeAnd,  // merge join on proximity

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

stageRecord := { stageType stage };
wordRecord := { wordType word; };
wordSet := set of wordType;
stageSet := set of stageType;

searchRecord :=
            RECORD
stageType       stage;
actionEnum      action;
//termType      term;
dataset(stageRecord) inputs{maxcount(maxTerms)};

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

//Modifiers for the connector/filter
distanceType    maxDistanceRightBeforeLeft;
distanceType    maxDistanceRightAfterLeft;
unsigned1       minMatches;
unsigned1       maxMatches;

            END;

childMatchRecord := RECORD
wordPosType         wpos;
wordPosType         wip;
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matches

matchRecord :=  RECORD
documentId          doc;
segmentType         segment;
wordPosType         wpos;
wordPosType         wip;
dataset(childMatchRecord) children{maxcount(MaxProximity)};
                END;

createChildMatch(wordPosType wpos, wordPosType wip) := transform(childMatchRecord, self.wpos := wpos; self.wip := wip);
SetOfInputs := set of dataset(matchRecord);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Functions which are helpful for hand constructing queries...

CmdReadWord(wordType word, sourceType source = 0, segmentType segment = 0, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.source := source;
                SELF.segment := segment;
                SELF.word := word;
                SELF.wordFlagMask := wordFlagMask;
                SELF.wordFlagCompare:= wordFlagCompare;
                SELF.maxWip := 1;
                SELF := []);

defineCmdTermCombineTerm(actionEnum action, stageSet inputs, distanceType maxDistanceRightBeforeLeft = 0, distanceType maxDistanceRightAfterLeft = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := action;
                SELF.inputs := StageSetToDataset(inputs);
                SELF.maxDistanceRightBeforeLeft := maxDistanceRightBeforeLeft;
                SELF.maxDistanceRightAfterLeft := maxDistanceRightAfterLeft;
                SELF.maxWip := 1;
                SELF.maxWipLeft := 1;
                SELF.maxWipRight := 1;
                SELF := []);

CmdTermAndTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.AndTerms, [leftStage, rightStage]);

CmdAndTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.AndTerms, stages);

CmdTermAndNotTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.AndNotTerms, [leftStage, rightStage]);

CmdTermAndNotTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.AndNotTerms, stages);

CmdMofNTerms(stageSet stages, unsigned minMatches, unsigned maxMatches = 999999999) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.MofNTerms;
                SELF.inputs := StageSetToDataset(stages);
                SELF.minMatches := minMatches;
                SELF.maxMatches := maxMatches;
                SELF.maxWip := 1;
                SELF := []);

CmdPhraseAnd(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.PhraseAnd, stages);

CmdProximityAnd(stageType leftStage, stageType rightStage, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) :=
    defineCmdTermCombineTerm(actionEnum.ProximityAnd, [leftStage, rightStage], maxDistanceRightBeforeLeft, maxDistanceRightAfterLeft);

CmdTermOrTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.OrTerms, [leftStage, rightStage]);

CmdOrTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.OrTerms, stages);


//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//---------------------------------------- Code for executing queries -----------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching helper functions

matchSingleWordFlags(wordIndex wIndex, searchRecord search) :=
    keyed(search.segment = 0 or wIndex.segment = search.segment, opt) AND
    ((wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);

matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word = search.word) AND
    matchSingleWordFlags(wIndex, search);

matchManyWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word in set(search.words, word)) AND
    matchSingleWordFlags(wIndex, search);

matchFirstWord(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR TS_docMatchesSource(wIndex.doc, search.source), opt);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWord(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.children := []
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left));

    return projected;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWordSet(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchManyWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.children := []
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
// RankMergeTerms

doRankMergeTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// M of N

doMofNTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, wip, dedup, mofn(search.minMatches, search.maxMatches));        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Join varieties - primarily for testing

//Note this testing transform wouldn't work correctly with proximity operators as inputs.
matchRecord createDenormalizedMatch(matchRecord l, dataset(matchRecord) matches) := transform

    wpos := min(matches, wpos);
    wend := max(matches, wpos + wip);

    self.wpos := wpos;
    self.wip := wend - wpos;
    self.children := normalize(matches, 1, createChildMatch(LEFT.wpos, LEFT.wip));
    self := l;
end;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndJoinTerms

doAndJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc) and (left.wpos <> right.wpos), createDenormalizedMatch(LEFT, ROWS(left)), sorted(doc, segment, wpos));
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotJoinTerms

doAndNotJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWS(left)), sorted(doc, segment, wpos), left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RankJoinTerms

doRankJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWS(left)), sorted(doc, segment, wpos), left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// M of N

doMofNJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWS(left)), sorted(doc, segment, wpos), mofn(search.minMatches, search.maxMatches));
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
        self := l;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, ROWS(LEFT)), sorted(doc, segment, wpos));

    return matches;
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

    createMatch(matchRecord l, matchRecord r) := function

        wpos := if(l.wpos < r.wpos, l.wpos, r.wpos);
        wend := if(l.wpos + l.wip > r.wpos + r.wip, l.wpos + l.wip, r.wpos + r.wip);

        rawLeftChildren := IF(exists(l.children), l.children, dataset(row(createChildMatch(l.wpos, l.wip))));
        rawRightChildren := IF(exists(r.children), r.children, dataset(row(createChildMatch(r.wpos, r.wip))));
        leftChildren := sorted(rawLeftChildren, wpos, wip, assert);
        rightChildren := sorted(rawRightChildren, wpos, wip, assert);
        anyOverlaps := exists(join(leftChildren, rightChildren,
                               overlaps(left.wpos, right) or overlaps(left.wpos+(left.wip-1), right) or
                               overlaps(right.wpos, left) or overlaps(right.wpos+(right.wip-1), left), all));

    //Check for any overlaps between the words, should be disjoint.
        matchRecord matchTransform := transform, skip(anyOverlaps)
            self.wpos := wpos;
            self.wip := wend - wpos;
            self.children := merge(leftChildren, rightChildren, dedup);
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

        wpos := if(l.wpos < r.wpos, l.wpos, r.wpos);
        wend := if(l.wpos + l.wip > r.wpos + r.wip, l.wpos + l.wip, r.wpos + r.wip);

        rawLeftChildren := IF(exists(l.children), l.children, dataset(row(createChildMatch(l.wpos, l.wip))));
        rawRightChildren := IF(exists(r.children), r.children, dataset(row(createChildMatch(r.wpos, r.wip))));
        leftChildren := sorted(rawLeftChildren, wpos, wip, assert);
        rightChildren := sorted(rawRightChildren, wpos, wip, assert);
        return exists(join(leftChildren, rightChildren,
                               overlaps(left.wpos, right) or overlaps(left.wpos+(left.wip-1), right) or
                               overlaps(right.wpos, left) or overlaps(right.wpos+(right.wip-1), left), all));
    end;

    matches := mergejoin(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT) and not anyOverlaps(LEFT,RIGHT), sorted(doc, segment, wpos));

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
        self.children := [];
        self := l;
    end;

    normalizedRecords := normalize(inputs[1], MAX(1, count(LEFT.children)), createNorm(left, counter));
    groupedNormalized := group(normalizedRecords, doc, segment);
    sortedNormalized := sort(groupedNormalized, wpos);
    dedupedNormalized := dedup(sortedNormalized, wpos, wip);
    return group(dedupedNormalized);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Rollup by document

doRollupByDocument(searchRecord search, dataset(matchRecord) input) := FUNCTION
    groupByDocument := group(input, doc);
    dedupedByDocument := rollup(groupByDocument, group, transform(matchRecord, self.doc := left.doc; self.segment := 0; self.wpos := 0; self.wip := 0; self := left));
    return dedupedByDocument;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStage(searchRecord search, SetOfInputs allInputs) := function
    inputs:= RANGE(allInputs, StageDatasetToSet(search.inputs));
    result := case(search.action,
        actionEnum.ReadWord             => doReadWord(search),
        actionEnum.ReadWordSet          => doReadWordSet(search),
        actionEnum.OrTerms              => doOrTerms(search, inputs),
        actionEnum.AndTerms             => doAndTerms(search, inputs),
        actionEnum.AndNotTerms          => doAndNotTerms(search, inputs),
        actionEnum.RankMergeTerms       => doRankMergeTerms(search, inputs),
        actionEnum.MofNTerms            => doMofNTerms(search, inputs),
        actionEnum.PhraseAnd            => doPhraseAnd(search, inputs),
        actionEnum.ProximityAnd         => doProximityAnd(search, inputs),
//      actionEnum.ProximityMergeAnd    => doProximityMergeAnd(search, inputs),
        actionEnum.AndJoinTerms         => doAndJoinTerms(search, inputs),
        actionEnum.AndNotJoinTerms      => doAndNotJoinTerms(search, inputs),
        actionEnum.RankJoinTerms        => doRankJoinTerms(search, inputs),
        actionEnum.MofNJoinTerms        => doMofNJoinTerms(search, inputs),
        actionEnum.RollupByDocument     => doRollupByDocument(search, allInputs[search.inputs[1].stage]),       // more efficient than way normalize is handled, but want to test both varieties
        actionEnum.NormalizeMatch       => doNormalizeMatch(search, inputs),
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

ExecuteQuery(dataset(searchRecord) queryDefinition, dataset(matchRecord) initialResults = dataset([], matchRecord)) := function

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


executeReadWord(wordType word, sourceType source = 0, segmentType segment = 0, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    doReadWord(row(CmdReadWord(word, source, segment, wordFlagMask, wordFlagCompare)));

executeAndTerms(SetOfInputs stages) :=
    doAndTerms(row(CmdAndTerms([])), stages);

executeAndNotTerms(SetOfInputs stages) :=
    doAndNotTerms(row(CmdTermAndNotTerms([])), stages);

executeMofNTerms(SetOfInputs stages, unsigned minMatches, unsigned maxMatches = 999999999) :=
    doMofNTerms(row(CmdMofNTerms([], minMatches, maxMatches)), stages);

executeOrTerms(SetOfInputs stages) :=
    doOrTerms(row(CmdOrTerms([])), stages);

executePhrase(SetOfInputs stages) :=
    doPhraseAnd(row(CmdPhraseAnd([])), stages);

executeProximity(SetOfInputs stages, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) :=
    doProximityAnd(row(CmdProximityAnd(0,0, maxDistanceRightBeforeLeft, maxDistanceRightAfterLeft)), stages);

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

PRULE forwardExpr := use(productionRecord, 'ExpressionRule');

ARULE term0
    := quotedword                               transform(searchParseRecord,
                                                    SELF.action := actionEnum.ReadWord;
                                                    SELF.word := $1[2..length($1)-1];
                                                    SELF := []
                                                )
    | 'CAPS' '(' SELF ')'                       transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper;
                                                    SELF.wordFlagCompare := wordFlags.hasUpper;
                                                    SELF := $3;
                                                )
    | 'NOCAPS' '(' SELF ')'                     transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper;
                                                    SELF.wordFlagCompare := 0;
                                                    SELF := $3;
                                                )
    | 'ALLCAPS' '(' SELF ')'                    transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper+wordFlags.hasLower;
                                                    SELF.wordFlagCompare := wordFlags.hasUpper;
                                                    SELF := $3;
                                                )
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

PRULE termList
    := forwardExpr                              transform(productionRecord, self.termCount := 1; self.actions := $1.actions)
    | SELF ',' forwardExpr                      transform(productionRecord, self.termCount := $1.termCount + 1; self.actions := $1.actions + $3.actions)
    ;

PRULE term1
    := term0                                    transform(productionRecord, self.termCount := 1; self.actions := dataset($1))
    | 'SET' '(' term0List ')'                       transform(productionRecord, self.termCount := 1; self.actions := dataset($3))
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
    | 'ANDNOT' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotTerms;
                                                            self.numInputs := $3.termCount;
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
    | 'ANDNOTJOIN' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotJoinTerms;
                                                            self.numInputs := $3.termCount;
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

wipRecord := { wordPosType wip; };

stageStackRecord := record
    stageType prevStage;
    dataset(stageRecord) stageStack{maxcount(MaxActions)};
    dataset(wipRecord) wipStack{maxcount(MaxActions)};
end;

nullStack := row(transform(stageStackRecord, self := []));

wordPosType _max(wordPosType l, wordPosType r) := if(l < r, r, l);

assignStageWip(searchParseRecord l, stageStackRecord r) := module

    shared stageType thisStage := r.prevStage + 1;
    shared stageType maxStage := count(r.stageStack);
    shared stageType minStage := maxStage+1-l.numInputs;
    shared thisInputs := r.stageStack[minStage..maxStage];

    shared maxLeftWip := r.wipStack[minStage].wip;
    shared maxRightWip := r.wipStack[maxStage].wip;
    shared maxChildWip := max(r.wipStack[minStage..maxStage], wip);
    shared sumMaxChildWip := sum(r.wipStack[minStage..maxStage], wip);

    shared thisMaxWip := case(l.action,
            actionEnum.ReadWord=>MaxWipIndexEntry,
            actionEnum.AndTerms=>maxChildWip,
            actionEnum.OrTerms=>maxChildWip,
            actionEnum.AndNotTerms=>maxLeftWip,
            actionEnum.PhraseAnd=>sumMaxChildWip,
            actionEnum.ProximityAnd=>_max(l.maxDistanceRightBeforeLeft,l.maxDistanceRightAfterLeft) + sumMaxChildWip,
            actionEnum.MofNTerms=>maxChildWip,
            maxChildWip);


    export searchParseRecord nextRow := transform
        self.stage := thisStage;
        self.inputs := thisInputs;
        self.maxWip := thisMaxWip;
        self.maxWipLeft := maxLeftWip;
        self.maxWipRight := maxRightWip;
        self.maxWipChild := maxChildWip;
        self := l;
    end;

    export stageStackRecord nextStack := transform
        self.prevStage := thisStage;
        self.stageStack := r.stageStack[1..maxStage-l.numInputs] + row(transform(stageRecord, self.stage := thisStage));
        self.wipStack := r.wipStack[1..maxStage-l.numInputs] + row(transform(wipRecord, self.wip := thisMaxWip;));
    end;
end;


sequenced := process(pnorm, nullStack, assignStageWip(left, right).nextRow, assignStageWip(left, right).nextStack);
return project(sequenced, transform(searchRecord, self := left));

end;


inputRecord := { string query{maxlength(2048)}; };

MaxResults := 10000;

processedRecord := record(inputRecord)
dataset(searchRecord) request{maxcount(MaxActions)};
dataset(simpleUserOutputRecord) result{maxcount(MaxResults)};
        end;


processedRecord doBatchExecute(inputRecord l) := transform
    request := parseQuery(l.query);
    self.request := request;
    self.result := ExecuteQuery(request);
    self := l;
end;


doSingleExecute(string queryText) := function
    request := parseQuery(queryText);
    result := ExecuteQuery(request);
    return result;
end;

q1 := dataset([

#if (0)
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
            'ANDNOT("one", "sheep", "black", "fish")',      // matches one, but none of the others

//Phrases
            'PHRASE("nonexistant1", "nonexistant2")',       // words don't occour
            'PHRASE("in", "are")',                          // doesn't occur, but words do
            'PHRASE("baa", "black")',                       // occurs, but
            'PHRASE("x", "y", "x", "x", "y")',              // a partial match, first - doesn't actually make it more complicatied to implement
            'PHRASE("james","arthur","stuart")',            // check that next occurence of stuart takes note of the change of document.
            'PHRASE(OR("black","white"),"sheep")',          // proximity on a non-word input
            'PHRASE("one", "for", OR(PHRASE("the","master"),PHRASE("the","dame"),PHRASE("the","little","boy")))',
                                                            // more complex again

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
            'ANDNOTJOIN("one", "sheep", "black", "fish")',      // matches one, but none of the others

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

            'RANKJOIN("sheep", "black")',
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

/*

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

*/

//SET should be equivalent to OR
            'SET("nonexistant1", "nonexistant2")',          // neither side matches
            'SET("nonexistant1", "sheep")',                 // RHS matches
            'SET("twinkle", "nonexistant2")',               // LHS matches
            'SET("twinkle", "twinkle")',                    // should dedup
#end

            'SET("sheep", "black")',                        // matches in same document
            'SET("sheep", "twinkle")',                      // matches in different documents
            'SET("one", "sheep", "sheep", "black", "fish")',    // matches in different documents
            'OR(SET("one", "sheep"), SET("sheep", "black", "fish"))',   // matches in different documents


//MORE:
// SET("a","b","c") to test stepping merging.
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


