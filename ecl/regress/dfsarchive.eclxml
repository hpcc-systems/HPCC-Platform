<Archive build="internal_10.0.31-closedown0"
         eclVersion="10.2.11"
         legacyImport="0"
         resolveFilesFromArchive="1"
         legacyWhen="0">
 <Query attributePath="ecl.dfsrecordof"/>
 <Dfs>
  <File ecl="RECORD&#10;  unsigned4 dg_parentid;&#10;  string10 dg_firstname;&#10;  string10 dg_lastname;&#10;  unsigned1 dg_prange;&#10; END;&#10;" name="~regress::single::DG_Parent.d00"/>
 </Dfs>
 <Module key="ecl" name="ecl">
  <Attribute key="dfsrecordof"
             name="dfsrecordof"
             sourcePath="ecl/dfsrecordof.ecl"
             ts="1761759182000000">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=file

#onwarning(7103, ignore);

import $.setup;
Files := setup.Files(false, false, false);

// Test compile-time field translation - recordof variant

Slim_OutRec := RECORD
    string10  DG_firstname;
    string10  DG_lastname;
END;

Extra_OutRec := RECORD
    string10  DG_firstname;
    string3   DG_lastname;
END;

// Test the LOOKUP attribute on recordof

#option (&apos;reportDFSinfo&apos;,2);

// Test no translation requested
no_trans_requested_record := RECORDOF(Files.DG_ParentFileOut, Files.DG_OutRec, LOOKUP(FALSE));
no_trans := DATASET(Files.DG_ParentFileOut,no_trans_requested_record,FLAT);
output(no_trans);

// Test no translation needed
no_trans_record := RECORDOF(Files.DG_ParentFileOut, Files.DG_OutRec, LOOKUP);
no_trans_needed := DATASET(Files.DG_ParentFileOut,no_trans_record,FLAT);
output(no_trans_needed);

// Test removing some fields
slimmed_record := RECORDOF(Files.DG_ParentFileOut, Slim_OutRec, LOOKUP);
slimmed := DATASET(Files.DG_ParentFileOut,slimmed_record,FLAT);
output(slimmed);

// changing fields (adding is not allowed)
changed_record := RECORDOF(Files.DG_ParentFileOut, Extra_OutRec, LOOKUP(TRUE));
changed := DATASET(Files.DG_ParentFileOut,changed_record,FLAT);
output(changed);

// Test OPT
notthere_record := RECORDOF(Files.DG_ParentFileOut, Extra_OutRec, LOOKUP, OPT);
notthere := DATASET(Files.DG_ParentFileOut+&apos;missing&apos;,notthere_record,FLAT,OPT);
output(notthere[1]);&#10;&#10;
  </Attribute>
 </Module>
 <Module key="ecl.setup" name="ecl.setup">
  <Attribute key="files"
             name="Files"
             sourcePath="./ecl/setup/files.ecl"
             ts="1761759182000000">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//skip type==setup TBD

import $.TS;
import ^ as root;
import std.str;

//define constants
EXPORT files(boolean multiPart, boolean useLocal, boolean useTranslation = false, string extraPrefix = &apos;&apos;) := module

EXPORT DG_MaxField          := 3;    // maximum number of fields to use building the data records
EXPORT DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs

                    // generates (#parents * DG_MaxChildren) records
EXPORT DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records

SHARED useDynamic := false;
SHARED useLayoutTrans := false;
SHARED useVarIndex := false;

#if (useDynamic=true)
SHARED STRING EmptyString := &apos;&apos; : STORED(&apos;dummy&apos;);
#option (&apos;allowVariableRoxieFilenames&apos;, 1);
#else
SHARED STRING EmptyString := &apos;&apos;;
#end

SHARED STRING _filePrefix := &apos;~regress::&apos; +
        MAP(multiPart =&gt; &apos;multi&apos;, &apos;single&apos;) +
        extraPrefix +
        &apos;::&apos; + EmptyString;

//Yuk cannot use MAP because that creates a string6
SHARED STRING _indexPrefix := &apos;~regress::&apos; +
        IF(multiPart AND useLocal, &apos;local&apos;, IF(multiPart, &apos;multi&apos;, &apos;single&apos;)) +
        extraPrefix +
        &apos;::&apos; + EmptyString;

EXPORT filePrefix := #IFDEFINED(root.filePrefix, _filePrefix);
EXPORT indexPrefix := #IFDEFINED(root.filePrefix, _indexPrefix);

wuid := Str.ToLowerCase(WORKUNIT);
wuidScope := IF(wuid &lt;&gt; &apos;&apos;, wuid, &apos;WORKUNIT&apos;);
EXPORT QueryFilePrefixId := __TARGET_PLATFORM__ + &apos;::&apos; + wuidScope + &apos;::&apos;;
EXPORT QueryFilePrefix := filePrefix + QueryFilePrefixId;

EXPORT DG_FileOut              := filePrefix + &apos;DG_&apos;;
EXPORT DG_IndexOut             := indexPrefix + &apos;DG_&apos;;
EXPORT DG_ParentFileOut        := filePrefix + &apos;DG_Parent.d00&apos;;
EXPORT DG_ParentFileOutGrouped := filePrefix + &apos;DG_ParentGrouped.d00&apos;;
EXPORT DG_ChildFileOut         := filePrefix + &apos;DG_Child.d00&apos;;
EXPORT DG_GrandChildFileOut    := filePrefix + &apos;DG_GrandChild.d00&apos;;
EXPORT DG_FetchFileName        := filePrefix + &apos;C.DG_FetchFile&apos;;
EXPORT DG_FetchFilePreloadName := filePrefix + &apos;C.DG_FetchFilePreload&apos;;
EXPORT DG_FetchFilePreloadIndexedName := filePrefix + &apos;C.DG_FetchFilePreloadIndexed&apos;;
EXPORT DG_FetchIndex1Name      := indexPrefix + &apos;DG_FetchIndex1&apos;;
EXPORT DG_FetchTransIndexName  := indexPrefix + &apos;DG_FetchTransIndex&apos;;
EXPORT DG_FetchIndexDiffName   := indexPrefix + &apos;DG_FetchIndexDiff&apos;;
EXPORT DG_KeyDiffIndex1Name    := indexPrefix + &apos;DG_KeyDiffIndex1&apos;;
EXPORT DG_KeyDiffIndex2Name    := indexPrefix + &apos;DG_KeyDiffIndex2&apos;;
EXPORT DG_QFetchIndexName     := indexPrefix + &apos;DG_QFetchIndex1&apos;;

EXPORT DG_DsFilename        := filePrefix + &apos;SerialLibraryDs&apos;;
EXPORT DG_DictFilename      := filePrefix + &apos;SerialLibraryDict&apos;;
EXPORT DG_DictKeyFilename   := indexPrefix + &apos;SerialLibraryKeyDict&apos;;
EXPORT DG_BookKeyFilename   := indexPrefix + &apos;SerialBookKey&apos;;

EXPORT SEQ_Filename              := filePrefix + &apos;Sequence&apos;;

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
EXPORT DG_QFetchIndex := INDEX(DG_FetchFile,{qstring25 Lname := Lname, qstring15 Fname := FName},{},DG_QFetchIndexName);

//These versions of the index are only used for KEYDIFF
EXPORT DG_KeyDiffIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING tfn := TRIM(Fname), state, STRING100 blobfield := fname, __filepos},DG_KeyDiffIndex1Name);
EXPORT DG_KeyDiffIndex2 := INDEX(DG_KeyDiffIndex1, DG_KeyDiffIndex2Name);

//This version is used for testing reading from a file requiring translation
EXPORT DG_FetchTransIndex := INDEX(DG_FetchFile,{Lname,Fname},{state, STRING tfn := TRIM(Fname), STRING100 blobfield {blob}:= fname, __filepos},DG_FetchTransIndexName);

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
  string emptyField { default(&apos;&apos;)};  // Makes all following field offsets variable...
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;

//DATASET declarations
EXPORT DG_BlankSet := dataset([{0,&apos;&apos;,&apos;&apos;,0}],DG_OutRec);

EXPORT DG_FlatFile      := DATASET(DG_FileOut+&apos;FLAT&apos;,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_FlatFileGrouped := DATASET(DG_FileOut+&apos;GROUPED&apos;,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,__GROUPED__);
EXPORT DG_FlatFileEvens := DATASET(DG_FileOut+&apos;FLAT_EVENS&apos;,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);

EXPORT DG_NormalIndexFile      := INDEX(DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexOut+&apos;INDEX&apos;);
EXPORT DG_NormalIndexFileEvens := INDEX(DG_FlatFileEvens, { DG_firstname; DG_lastname; }, { DG_Prange, filepos } ,DG_IndexOut+&apos;INDEX_EVENS&apos;);

EXPORT DG_TransIndexFile      := INDEX(DG_FlatFile, { DG_firstname, DG_lastname }, { DG_ChildID := filepos, DG_Prange, filepos }, DG_IndexOut+&apos;TRANS_INDEX&apos;);
EXPORT DG_TransIndexFileEvens := INDEX(DG_FlatFileEvens, { DG_firstname, DG_lastname }, { DG_ChildID := filepos, DG_Prange, filepos } ,DG_IndexOut+&apos;TRANS_INDEX_EVENS&apos;);

indexName := IF(useTranslation, __nameof__(DG_TransIndexFile), __nameof__(DG_NormalIndexFile));
EXPORT DG_indexFile      := INDEX(DG_NormalIndexFile, indexName);

indexName := IF(useTranslation, __nameof__(DG_TransIndexFileEvens), __nameof__(DG_NormalIndexFileEvens));
EXPORT DG_indexFileEvens := INDEX(DG_NormalIndexFileEvens, indexName);


EXPORT DG_KeyedIndexFile      := INDEX(DG_FlatFile, { DG_firstname, DG_lastname, DG_Prange}, { filepos }, DG_IndexOut+&apos;KEYED_INDEX&apos;, fileposition(false));
EXPORT DG_KeyedIndexFileDelta := INDEX(DG_FlatFile, { DG_firstname, DG_lastname, DG_Prange}, { UNSIGNED8 filepos := filepos + 1 }, DG_IndexOut+&apos;KEYED_INDEX_DELTA&apos;, fileposition(false));
//Combine the two files above with an implicit superkey
EXPORT DG_DupKeyedIndexFile   := INDEX(DG_FlatFile, { DG_firstname, DG_lastname, DG_Prange}, { filepos }, &apos;{&apos; + DG_IndexOut+&apos;KEYED_INDEX&apos; + &apos;,&apos; + DG_IndexOut+&apos;KEYED_INDEX_DELTA&apos; + &apos;}&apos;, fileposition(false));
EXPORT DG_DupKeyedIndexSuperFileName   := DG_IndexOut+&apos;KEYED_INDEX_DUP&apos;;

EXPORT DG_CSVFile   := DATASET(DG_FileOut+&apos;CSV&apos;,DG_OutRec,CSV);
EXPORT DG_XMLFile   := DATASET(DG_FileOut+&apos;XML&apos;,DG_OutRec,XML);

EXPORT DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

EXPORT DG_VarFile   := DATASET(DG_FileOut+&apos;VAR&apos;,DG_VarOutRecPlus,FLAT);

EXPORT DG_NormalVarIndex  := INDEX(DG_VarFile, { DG_firstname; DG_lastname; __filepos } ,DG_IndexOut+&apos;VARINDEX&apos;); // THIS IS NOT VARIABLE - stupid test!
EXPORT DG_TransVarIndex  := INDEX(DG_VarFile, { DG_firstname; DG_lastname; }, { DGextra := DG_lastname; __filepos } ,DG_IndexOut+&apos;TRANS_VARINDEX&apos;);
EXPORT DG_IntIndex  := INDEX(DG_VarFile, { DG_parentID; DG_firstname =&gt; STRING DG_lastname := DG_lastname; __filepos } ,DG_IndexOut+&apos;INTINDEX&apos;);

indexName := IF(useTranslation, __nameof__(DG_TransVarIndex), __nameof__(DG_NormalVarIndex));
EXPORT DG_VarIndex  := INDEX(DG_NormalVarIndex, indexName);

EXPORT DG_ParentFile  := DATASET(DG_ParentFileOut,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_ChildFile   := DATASET(DG_ChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
EXPORT DG_GrandChildFile := DATASET(DG_GrandChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);

//define data atoms - each set has 16 elements
EXPORT SET OF STRING10 DG_Fnames := [&apos;DAVID&apos;,&apos;CLAIRE&apos;,&apos;KELLY&apos;,&apos;KIMBERLY&apos;,&apos;PAMELA&apos;,&apos;JEFFREY&apos;,&apos;MATTHEW&apos;,&apos;LUKE&apos;,
                              &apos;JOHN&apos; ,&apos;EDWARD&apos;,&apos;CHAD&apos; ,&apos;KEVIN&apos;   ,&apos;KOBE&apos;  ,&apos;RICHARD&apos;,&apos;GEORGE&apos; ,&apos;DIRK&apos;];
EXPORT SET OF STRING10 DG_Lnames := [&apos;BAYLISS&apos;,&apos;DOLSON&apos;,&apos;BILLINGTON&apos;,&apos;SMITH&apos;   ,&apos;JONES&apos;   ,&apos;ARMSTRONG&apos;,&apos;LINDHORFF&apos;,&apos;SIMMONS&apos;,
                              &apos;WYMAN&apos;  ,&apos;MORTON&apos;,&apos;MIDDLETON&apos; ,&apos;NOWITZKI&apos;,&apos;WILLIAMS&apos;,&apos;TAYLOR&apos;   ,&apos;CHAPMAN&apos;  ,&apos;BRYANT&apos;];
EXPORT SET OF UNSIGNED1 DG_PrangeS := [1, 2, 3, 4, 5, 6, 7, 8,
                                9,10,11,12,13,14,15,16];
EXPORT SET OF STRING10 DG_Streets := [&apos;HIGH&apos;  ,&apos;CITATION&apos;  ,&apos;MILL&apos;,&apos;25TH&apos; ,&apos;ELGIN&apos;    ,&apos;VICARAGE&apos;,&apos;YAMATO&apos; ,&apos;HILLSBORO&apos;,
                               &apos;SILVER&apos;,&apos;KENSINGTON&apos;,&apos;MAIN&apos;,&apos;EATON&apos;,&apos;PARK LANE&apos;,&apos;HIGH&apos;    ,&apos;POTOMAC&apos;,&apos;GLADES&apos;];
EXPORT SET OF UNSIGNED1 DG_ZIPS := [101,102,103,104,105,106,107,108,
                             109,110,111,112,113,114,115,116];
EXPORT SET OF UNSIGNED1 DG_AGES := [31,32,33,34,35,36,37,38,
                             39,40,41,42,43,44,45,56];
EXPORT SET OF STRING2 DG_STATES := [&apos;FL&apos;,&apos;GA&apos;,&apos;SC&apos;,&apos;NC&apos;,&apos;TX&apos;,&apos;AL&apos;,&apos;MS&apos;,&apos;TN&apos;,
                             &apos;KY&apos;,&apos;CA&apos;,&apos;MI&apos;,&apos;OH&apos;,&apos;IN&apos;,&apos;IL&apos;,&apos;WI&apos;,&apos;MN&apos;];
EXPORT SET OF STRING3 DG_MONTHS := [&apos;JAN&apos;,&apos;FEB&apos;,&apos;MAR&apos;,&apos;APR&apos;,&apos;MAY&apos;,&apos;JUN&apos;,&apos;JUL&apos;,&apos;AUG&apos;,
                             &apos;SEP&apos;,&apos;OCT&apos;,&apos;NOV&apos;,&apos;DEC&apos;,&apos;ABC&apos;,&apos;DEF&apos;,&apos;GHI&apos;,&apos;JKL&apos;];

//----------------------------- Text search definitions ----------------------------------

EXPORT NameWordIndex() := indexPrefix + &apos;wordIndex&apos; + IF(useLocal, &apos;_Local&apos;, &apos;&apos;) + IF(useTranslation, &apos;_Trans&apos;, &apos;&apos;) ;
EXPORT NameSearchIndex := indexPrefix + &apos;searchIndex&apos;;
EXPORT NameSearchSource := indexPrefix + &apos;searchSource&apos;;
EXPORT getWordIndex() := INDEX(TS.textSearchIndex, NameWordIndex());
EXPORT getSearchIndex() := INDEX(TS.textSearchIndex, NameSearchIndex);
EXPORT getSearchIndexVariant(string variant) := INDEX(TS.textSearchIndex, NameSearchIndex + IF(variant != &apos;&apos;, &apos;_&apos; + variant, &apos;&apos;));
EXPORT getOptSearchIndexVariant(string variant) := INDEX(TS.textSearchIndex, NameSearchIndex + IF(variant != &apos;&apos;, &apos;_&apos; + variant, &apos;&apos;), OPT);

EXPORT getSearchSuperIndex() := INDEX(TS.textSearchIndex, &apos;{&apos; + NameSearchIndex + &apos;,&apos; + NameWordIndex() + &apos;}&apos;);
EXPORT getSearchSource() := DATASET(NameSearchSource, TS.textSourceRecord, THOR);

EXPORT SeqRecord := { unsigned seq; };
EXPORT SeqReadRecord :=
  RECORD(SeqRecord)
    unsigned8 filepos{virtual(fileposition)};
    unsigned8 localfilepos{virtual(localfileposition)};
  END;

EXPORT SeqFile := DATASET(SEQ_Filename, SeqReadRecord, THOR);
EXPORT SeqDupFile := DATASET(&apos;{&apos; + SEQ_Filename + &apos;,&apos; + SEQ_Filename + &apos;}&apos;, SeqReadRecord, THOR);

END;&#10;
  </Attribute>
  <Attribute key="ts"
             name="TS"
             sourcePath="./ecl/setup/ts.ecl"
             ts="1764856660000000">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//skip type==setup TBD

//define constants
EXPORT TS := module

EXPORT MaxTerms             := 50;
EXPORT MaxStages            := 50;
EXPORT MaxProximity         := 10;
EXPORT MaxWildcard          := 1000;
EXPORT MaxMatchPerDocument  := 1000;
EXPORT MaxFilenameLength        := 255;
EXPORT MaxActions           := 255;
EXPORT MaxTagNesting        := 40;
EXPORT MaxColumnsPerLine := 10000;          // used to create a pseudo document position

EXPORT kindType         := enum(unsigned1, UnknownEntry=0, TextEntry, OpenTagEntry, CloseTagEntry, OpenCloseTagEntry, CloseOpenTagEntry);
EXPORT sourceType       := unsigned2;
EXPORT wordCountType    := unsigned8;
EXPORT segmentType      := unsigned1;
EXPORT wordPosType      := unsigned8;
EXPORT docPosType       := unsigned8;
EXPORT documentId       := unsigned8;
EXPORT termType         := unsigned1;
EXPORT distanceType     := integer8;
EXPORT indexWipType     := unsigned1;
EXPORT wipType          := unsigned8;
EXPORT stageType        := unsigned1;
EXPORT dateType         := unsigned8;

EXPORT sourceType docid2source(documentId x) := (x &gt;&gt; 48);
EXPORT documentId docid2doc(documentId x) := (x &amp; 0xFFFFFFFFFFFF);
EXPORT documentId createDocId(sourceType source, documentId doc) := (documentId)(((unsigned8)source &lt;&lt; 48) | doc);
EXPORT boolean      docMatchesSource(documentId docid, sourceType source) := (docid between createDocId(source,0) and (documentId)(createDocId(source+1,0)-1));

EXPORT wordType := string20;
EXPORT wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);

EXPORT wordIdType       := unsigned4;

EXPORT textSearchIndex  := index({ kindType kind, wordType word, documentId doc, segmentType segment, wordPosType wpos, indexWipType wip } , { wordFlags flags, wordType original, docPosType dpos}, &apos;~DoesNotExist&apos;);
EXPORT wordIndexRecord := recordof(textSearchIndex);

EXPORT textSourceRecord := record
    kindType     kind;
    wordType     word;
    documentId   doc;
    segmentType  segment;
    wordPosType  wpos;
END;

END;&#10;
  </Attribute>
 </Module>
 <Module key="std" name="std"/>
 <Option name="spanmultiplecpp" value="0"/>
 <Option name="savecpptempfiles" value="1"/>
 <Option name="eclcc_compiler_version" value="10.2.11"/>
 <Option name="debugquery" value="1"/>
</Archive>
