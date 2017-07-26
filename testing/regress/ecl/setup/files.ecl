/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

import $.TS;

//define constants
EXPORT files(boolean multiPart, boolean useLocal, boolean useTranslation = false) := module

EXPORT DG_MaxField          := 3;    // maximum number of fields to use building the data records
EXPORT DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs

                    // generates (#parents * DG_MaxChildren) records
EXPORT DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records

SHARED useDynamic := false;
SHARED useLayoutTrans := false;
SHARED useVarIndex := false;

#if (useDynamic=true)
SHARED STRING EmptyString := '' : STORED('dummy');
#option ('allowVariableRoxieFilenames', 1);
#else
SHARED STRING EmptyString := '';
#end

EXPORT STRING filePrefix := MAP(
        multiPart => 'multi',
        'single') + EmptyString;

//Yuk cannot use MAP because that creates a string6        
EXPORT STRING indexPrefix := IF(multiPart AND useLocal, 'local', 
                             IF(multiPart, 'multi',
                             'single')) + EmptyString;
        
EXPORT DG_FileOut           := '~REGRESS::' + filePrefix + '::DG_';
EXPORT DG_IndexOut           := '~REGRESS::' + indexPrefix + '::DG_';
EXPORT DG_ParentFileOut     := '~REGRESS::' + filePrefix + '::DG_Parent.d00';
EXPORT DG_ParentFileOutGrouped     := '~REGRESS::' + filePrefix + '::DG_ParentGrouped.d00';
EXPORT DG_ChildFileOut      := '~REGRESS::' + filePrefix + '::DG_Child.d00';
EXPORT DG_GrandChildFileOut := '~REGRESS::' + filePrefix + '::DG_GrandChild.d00';
EXPORT DG_FetchFileName     := '~REGRESS::' + filePrefix + '::C.DG_FetchFile';
EXPORT DG_FetchFilePreloadName := '~REGRESS::' + filePrefix + '::C.DG_FetchFilePreload';
EXPORT DG_FetchFilePreloadIndexedName := '~REGRESS::' + filePrefix + '::C.DG_FetchFilePreloadIndexed';
EXPORT DG_FetchIndex1Name   := '~REGRESS::' + indexPrefix + '::DG_FetchIndex1';
EXPORT DG_FetchTransIndexName   := '~REGRESS::' + indexPrefix + '::DG_FetchTransIndex';
EXPORT DG_FetchIndexDiffName:= '~REGRESS::' + indexPrefix + '::DG_FetchIndexDiff';
EXPORT DG_KeyDiffIndex1Name   := '~REGRESS::' + indexPrefix + '::DG_KeyDiffIndex1';
EXPORT DG_KeyDiffIndex2Name   := '~REGRESS::' + indexPrefix + '::DG_KeyDiffIndex2';

EXPORT DG_DsFilename        := '~REGRESS::' + filePrefix + '::SerialLibraryDs';
EXPORT DG_DictFilename      := '~REGRESS::' + filePrefix + '::SerialLibraryDict';
EXPORT DG_DictKeyFilename   := '~REGRESS::' + indexPrefix + '::SerialLibraryKeyDict';
EXPORT DG_BookKeyFilename   := '~REGRESS::' + indexPrefix + '::SerialBookKey';

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

//The standard version of the index


EXPORT DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING tfn := TRIM(Fname), state, STRING100 blobfield {blob}:= fname, __filepos},DG_FetchIndex1Name);

//These versions of the index are only used for KEYDIFF
EXPORT DG_KeyDiffIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING tfn := TRIM(Fname), state, STRING100 blobfield := fname, __filepos},DG_KeyDiffIndex1Name);
EXPORT DG_KeyDiffIndex2 := INDEX(DG_KeyDiffIndex1, DG_KeyDiffIndex2Name);

//This version is used for testing reading from a file requiring translation 
EXPORT DG_FetchTransIndex := INDEX(DG_FetchFile,{Lname,Fname},{STRING tfn := TRIM(Fname), state, STRING100 blobfield {blob}:= fname, __filepos},DG_FetchTransIndexName);

indexName := IF(useTranslation, __nameof__(DG_FetchTransIndex), __nameof__(DG_FetchIndex1));
EXPORT DG_FetchIndex := INDEX(DG_FetchIndex1,indexName);

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
  string emptyField { default('')};  // Makes all following field offsets variable... 
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;

//DATASET declarations
EXPORT DG_BlankSet := dataset([{0,'','',0}],DG_OutRec);

EXPORT DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_FlatFileGrouped := DATASET(DG_FileOut+'GROUPED',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,__GROUPED__);
EXPORT DG_FlatFileEvens := DATASET(DG_FileOut+'FLAT_EVENS',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);

EXPORT DG_NormalIndexFile      := INDEX(DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexOut+'INDEX');
EXPORT DG_NormalIndexFileEvens := INDEX(DG_FlatFileEvens, { DG_firstname; DG_lastname; }, { DG_Prange, filepos } ,DG_IndexOut+'INDEX_EVENS');

EXPORT DG_TransIndexFile      := INDEX(DG_FlatFile, { DG_lastname, DG_firstname }, { DG_Prange, filepos }, DG_IndexOut+'TRANS_INDEX');
EXPORT DG_TransIndexFileEvens := INDEX(DG_FlatFileEvens, { DG_lastname, DG_firstname }, { DG_Prange, filepos } ,DG_IndexOut+'TRANS_INDEX_EVENS');

indexName := IF(useTranslation, __nameof__(DG_TransIndexFile), __nameof__(DG_NormalIndexFile));
EXPORT DG_indexFile      := INDEX(DG_NormalIndexFile, indexName);

indexName := IF(useTranslation, __nameof__(DG_TransIndexFileEvens), __nameof__(DG_NormalIndexFileEvens));
EXPORT DG_indexFileEvens := INDEX(DG_NormalIndexFileEvens, indexName);

EXPORT DG_KeyedIndexFile      := INDEX(DG_FlatFile, { DG_firstname, DG_lastname, DG_Prange}, { filepos }, DG_IndexOut+'KEYED_INDEX');

EXPORT DG_CSVFile   := DATASET(DG_FileOut+'CSV',DG_OutRec,CSV);
EXPORT DG_XMLFile   := DATASET(DG_FileOut+'XML',DG_OutRec,XML);

EXPORT DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

EXPORT DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT);

EXPORT DG_NormalVarIndex  := INDEX(DG_VarFile, { DG_firstname; DG_lastname; __filepos } ,DG_IndexOut+'VARINDEX');
EXPORT DG_TransVarIndex  := INDEX(DG_VarFile, { DG_lastname; DG_firstname; __filepos } ,DG_IndexOut+'TRANS_VARINDEX');

indexName := IF(useTranslation, __nameof__(DG_TransVarIndex), __nameof__(DG_NormalVarIndex));
EXPORT DG_VarIndex  := INDEX(DG_NormalVarIndex, indexName);

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

//----------------------------- Text search definitions ----------------------------------

EXPORT NameWordIndex() := '~REGRESS::' + indexPrefix + '::wordIndex' + IF(useLocal, '_Local', '');
EXPORT NameSearchIndex      := '~REGRESS::' + indexPrefix + '::searchIndex';
EXPORT getWordIndex() := INDEX(TS.textSearchIndex, NameWordIndex());
EXPORT getSearchIndex() := INDEX(TS.textSearchIndex, NameSearchIndex);
EXPORT getSearchSuperIndex() := INDEX(TS.textSearchIndex, '{' + NameSearchIndex + ',' + NameWordIndex() + '}');

END;
