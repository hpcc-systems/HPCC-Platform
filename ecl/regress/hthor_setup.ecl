#option('foldConstantDatasets', 0);
#option('pickBestEngine', 0);
#option('layoutTranslationEnabled', 0);
#option('targetClusterType', 'hthor');
#option('testIgnoreMaxLength',true);
prefix := 'hthor';
useLayoutTrans := false;
useLocal := false;
usePayload := false;
useVarIndex := false;
useDynamic := false;
setupTextFileLocation := '.::c$::edata::testing::ecl::files';
#line(0)
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
DG_IntegerDatasetName:= '~REGRESS::' + filePrefix + '::DG_IntegerFile';
DG_IntegerIndexName  := '~REGRESS::' + filePrefix + '::DG_IntegerIndex';

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
                              'WYMAN'  ,'MORTON','MIDDLETON' ,'NOWITZKI','WILLIAMS','TAYLOR'   ,'CHAPMAN'  ,'BRYANT']; 
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


t_personfile := DATASET('t_personfile', RECORD
  unsigned integer4 hhid;
  unsigned integer4 personid;
  string20 firstname;
  string20 lastname;
  string20 middlename;
  unsigned integer1 age;
  unsigned integer8 ssn;
END, THOR);

t_tradesfile := DATASET('t_tradesfile', RECORD
  unsigned integer4 personid;
  string20 tradeid;
  real4 amount;
  string8 date;
END, THOR);

t_hhfile := DATASET('t_hhfile', RECORD
  unsigned integer4 hhid;
  string2 State;
  string5 zip;
  string20 City;
  string40 street;
  unsigned integer4 houseNumber;
END, THOR);


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
TS_MaxColumnsPerLine := 10000;          // used to create a pseudo document position

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
TS_NameSearchIndex      := '~REGRESS::' + filePrefix + '::TS_searchIndex';

TS_wordIndex        := index({ TS_kindType kind, TS_wordType word, TS_documentId doc, TS_segmentType segment, TS_wordPosType wpos, TS_indexWipType wip } , { TS_wordFlags flags, TS_wordType original, TS_docPosType dpos}, TS_NameWordIndex);
TS_searchIndex      := index(TS_wordIndex, TS_NameSearchIndex);

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


//record structures
DG_NestedIntegerRecord := RECORD
  INTEGER4 i4;
  UNSIGNED3 u3;
END;

DG_IntegerRecord := RECORD
    INTEGER6    i6;
    DG_NestedIntegerRecord nested;
    integer5    i5;
    integer3    i3;
END;

DG_IntegerDataset := DATASET(DG_IntegerDatasetName, DG_IntegerRecord, thor);
DG_IntegerIndex := INDEX(DG_IntegerDataset, { i6, nested }, { DG_IntegerDataset }, DG_IntegerIndexName);


#line(0)
#line(0)
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

import * FROM lib_fileservices;

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
    { 'Maltings Road', 'SW1A0AA', 1720, 
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
