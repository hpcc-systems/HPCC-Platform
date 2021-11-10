/*##############################################################################

    Copyright (C) 2021 HPCC Systems®.

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

//nohthor
//noroxie
//timeout 1200

//class=spray
//class=copy
//class=dfuplus

//version mode='singleFieldSingleSmallRecord'
//version mode='singleFieldSingleLargeRecord'
//version mode='singleFieldSingleHugeRecord'
//version mode='singleFieldOneSmallOneLargeRecord'
//version mode='singleFieldOneLargeOneSmallRecord'
//version mode='singleFieldOneSmallOneHugeRecord'
//version mode='singleFieldOneHugeOneSmallRecord'
//version mode='singleFieldMultipleRecords'
//version mode='singleFieldMultipleLargeRecords'
//version mode='singleFieldMultipleHugeRecords'
//version mode='singleFieldMultipleRecordsSpecTerminator'
//version mode='multipleFieldMultipleRecords'
//version mode='multipleFieldMultipleRecordsSpecTerminator'
//version mode='multipleFieldMultipleRecordsSpecSeparator'
//version mode='multipleFieldMultipleRecordsSpecSeparatorTerminator'

#option('overrideSkewError', '1.0');

import Std.Str as Str;
import Std.File AS FileServices;
import $.setup;
import ^ as root;

prefix := setup.Files(false, false).QueryFilePrefix;

string mode := #IFDEFINED(root.mode, 'singleFieldSingleLargeRecord');

dropzonePath := '';// Use relative paths

unsigned VERBOSE := 0;
unsigned CLEAN_UP := 1;

unsigned DEFAULT_REC_SIZE := 8192;
unsigned SMALL_REC_SIZE := 25;
unsigned LARGE_REC_SIZE := 265000;
unsigned HUGE_REC_SIZE := 5000000;
unsigned SMALL_REC_FIRST := 1;
unsigned LARGE_REC_FIRST := 2;
unsigned REC_COUNT := 100;

t := '0123456789';

String genFiled(Integer i) := Function
  unsigned d := i / 10;
  unsigned r := i % 10;
  RETURN Str.Repeat(t, d) + t[1..r];
end;


SingleFieldLayout := RECORD
  String field;
END;

emptySingleFiled := DATASET([{''}], SingleFieldLayout);

SingleFieldLayout genSingleFieldRecs(SingleFieldLayout L, INTEGER recordIndex, INTEGER smallRecordIndex, INTEGER smallRecSize, INTEGER largeREcSize) := TRANSFORM
	SELF.field := IF(recordIndex = smallRecordIndex, genfiled(smallRecSize), genfiled(largeREcSize));
END;


MultipleFieldLayout := RECORD
  UNSIGNED 	id;
  STRING   	field1;
  STRING	field2;
  STRING 	field3;
end;


emptyMultipleFiled := DATASET([{0, '', '', ''}], MultipleFieldLayout);

MultipleFieldLayout genMultipleFieldRecs(emptyMultipleFiled L, INTEGER recordIndex, INTEGER smallRecordIndex, INTEGER smallRecSize, INTEGER largeREcSize) := TRANSFORM
    SELF.id := 	recordIndex;
    SELF.field1 := IF(recordIndex = smallRecordIndex, genfiled(smallRecSize), genfiled(largeREcSize));
    SELF.field2 := IF(recordIndex = smallRecordIndex, genfiled(smallRecSize), genfiled(largeREcSize));
    SELF.field3 := IF(recordIndex = smallRecordIndex, genfiled(smallRecSize), genfiled(largeREcSize));
END;



#if (mode = 'singleFieldSingleSmallRecord')
    singleSmallFieldSingleRecord := DATASET(1,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := genfiled(SMALL_REC_SIZE);
                        )
              ,DISTRIBUTED
              );

    dsSetup := singleSmallFieldSingleRecord;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldSingleLargeRecord')

    singleLargeFieldSingleRecord := DATASET(1,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := genfiled(LARGE_REC_SIZE);
                        )
              ,DISTRIBUTED
              );
    dsSetup := singleLargeFieldSingleRecord;
    recSize := LARGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldSingleHugeRecord' )
    singleFieldSingleHugeRecord := DATASET(1,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := genfiled(HUGE_REC_SIZE);
                        )
              ,DISTRIBUTED
              );
    dsSetup := singleFieldSingleHugeRecord;
    recSize := HUGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldOneSmallOneLargeRecord')

    singleFieldOneSmallOneLargeRecord := NORMALIZE(emptySingleFiled, 2, genSingleFieldRecs(LEFT, COUNTER, SMALL_REC_FIRST, SMALL_REC_SIZE, LARGE_REC_SIZE) );

    dsSetup := singleFieldOneSmallOneLargeRecord;
    recSize := LARGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldOneLargeOneSmallRecord')

    singleFieldOneLargeOneSmallRecord := NORMALIZE(emptySingleFiled, 2, genSingleFieldRecs(LEFT, COUNTER, LARGE_REC_FIRST, SMALL_REC_SIZE, LARGE_REC_SIZE) );

    dsSetup := singleFieldOneLargeOneSmallRecord;
    recSize := LARGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif ( mode = 'singleFieldOneSmallOneHugeRecord')

    singleFieldOneSmallOneHugeRecord := NORMALIZE(emptySingleFiled, 2, genSingleFieldRecs(LEFT, COUNTER, SMALL_REC_FIRST, SMALL_REC_SIZE, HUGE_REC_SIZE) );

    dsSetup := singleFieldOneSmallOneHugeRecord;
    recSize := HUGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif ( mode = 'singleFieldOneHugeOneSmallRecord')

    singleFieldOneHugeOneSmallRecord := NORMALIZE(emptySingleFiled, 2, genSingleFieldRecs(LEFT, COUNTER, LARGE_REC_FIRST, SMALL_REC_SIZE, HUGE_REC_SIZE) );

    dsSetup := singleFieldOneHugeOneSmallRecord;
    recSize := HUGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldMultipleRecords')

    singleFieldMultipleRecords := DATASET(REC_COUNT,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := INTFORMAT(COUNTER,5,1) + ':'+ genfiled(RANDOM() % 150);
                        )
              ,DISTRIBUTED
              );

    dsSetup := singleFieldMultipleRecords : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldMultipleLargeRecords')

    singleFieldMultipleLargeRecords := DATASET(REC_COUNT,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := INTFORMAT(COUNTER,5,1) + ':' + genfiled(RANDOM() % LARGE_REC_SIZE);
                        )
              ,DISTRIBUTED
              );

    dsSetup := singleFieldMultipleLargeRecords : INDEPENDENT;
    recSize := LARGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldMultipleHugeRecords')

    singleFieldMultipleHugeRecords := DATASET(REC_COUNT,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := INTFORMAT(COUNTER,5,1) + ':' + genfiled(RANDOM() % HUGE_REC_SIZE);
                        )
              ,DISTRIBUTED    
              );

    dsSetup := singleFieldMultipleHugeRecords : INDEPENDENT;
    recSize := HUGE_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'singleFieldMultipleRecordsSpecTerminator')

    singleFieldMultipleRecords := DATASET(REC_COUNT,
                                   TRANSFORM({SingleFieldLayout},
                         SELF.field := INTFORMAT(COUNTER,5,1) + ':'+ genfiled(RANDOM() % 150);
                        )
              ,DISTRIBUTED
              );

    dsSetup := singleFieldMultipleRecords : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := (>STRING<) x'01';
    String csvSeparator := ',';

#elseif (mode = 'multipleFieldMultipleRecords')

    multipleFieldMultipleRecords := DATASET(REC_COUNT,
                                        TRANSFORM({MultipleFieldLayout},
                                            SELF.id := COUNTER,                         
                                            SELF.field1 := genfiled(RANDOM() % 150),
                                            SELF.field2 := genfiled(RANDOM() % 150),
                                            SELF.field3 := genfiled(RANDOM() % 150);
                                        )
                                      ,DISTRIBUTED
                                  );

    dsSetup := multipleFieldMultipleRecords : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := ',';

#elseif (mode = 'multipleFieldMultipleRecordsSpecTerminator')

    multipleFieldMultipleRecordsSpecTerminator := DATASET(REC_COUNT,
                                        TRANSFORM({MultipleFieldLayout},
                                            SELF.id := COUNTER,                         
                                            SELF.field1 := genfiled(RANDOM() % 150),
                                            SELF.field2 := genfiled(RANDOM() % 150),
                                            SELF.field3 := genfiled(RANDOM() % 150);
                                        )
                                      ,DISTRIBUTED
                                  );

    dsSetup := multipleFieldMultipleRecordsSpecTerminator : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := (>STRING<) x'01';
    String csvSeparator := ',';

#elseif (mode = 'multipleFieldMultipleRecordsSpecSeparator')

    multipleFieldMultipleRecordsSpecSeparator := DATASET(REC_COUNT,
                                        TRANSFORM({MultipleFieldLayout},
                                            SELF.id := COUNTER,                         
                                            SELF.field1 := genfiled(RANDOM() % 150),
                                            SELF.field2 := genfiled(RANDOM() % 150),
                                            SELF.field3 := genfiled(RANDOM() % 150);
                                        )
                                      ,DISTRIBUTED
                                  );

    dsSetup := multipleFieldMultipleRecordsSpecSeparator : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := '\n';
    String csvSeparator := (>STRING<) x'01';

#elseif (mode = 'multipleFieldMultipleRecordsSpecSeparatorTerminator')

    multipleFieldMultipleRecordsSpecSeparatorTerminator := DATASET(REC_COUNT,
                                        TRANSFORM({MultipleFieldLayout},
                                            SELF.id := COUNTER,                         
                                            SELF.field1 := genfiled(RANDOM() % 150),
                                            SELF.field2 := genfiled(RANDOM() % 150),
                                            SELF.field3 := genfiled(RANDOM() % 150);
                                        )
                                      ,DISTRIBUTED
                                  );

    dsSetup := multipleFieldMultipleRecordsSpecSeparatorTerminator : INDEPENDENT;
    recSize := DEFAULT_REC_SIZE;
    String csvTerminator := (>STRING<) x'02';
    String csvSeparator := (>STRING<) x'01';

#end



#if (__CONTAINERIZED__)
    SprayClusterName := 'data';
    myRemoteEspFsURL := 'http://eclwatch:8010/FileSpray';
#else
    SprayClusterName := 'mythor';
    myRemoteEspFsURL := 'http://127.0.0.1:8010/FileSpray';
#end

sprayPrepFileName := prefix + 'spray_prep';

//  Create a logical file
setupFile := output(dsSetup, , sprayPrepFileName, CSV(HEADING('',''), SEPARATOR(csvSeparator), TERMINATOR(csvTerminator), QUOTE('"')), OVERWRITE);

desprayOutFileName := dropzonePath + WORKUNIT + '-spray_input';

sprayOutFileName := prefix + 'spray_csv';

rec := RECORD
  string result;
  string msg;
end;


// Despray it to default drop zone
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName
                      ,DESTINATIONPLANE := 'mydropzone'
                      ,DESTINATIONPATH := desprayOutFileName
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Despray Pass';
end;

dst1 := NOFOLD(DATASET([{'', ''}], rec));
p1 := NOTHOR(PROJECT(NOFOLD(dst1), despray(LEFT)));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    desprayOut := output(c1);
#else
    desprayOut := output(c1, {result});
#end


rec spray(rec l) := TRANSFORM
    SELF.msg := FileServices.fSprayVariable(
                    SOURCEPLANE := 'mydropzone',
                    SOURCEPATH := desprayOutFileName,
                    SOURCEMAXRECORDSIZE := recSize,
                    SOURCECSVTERMINATE := csvTerminator,
                    SOURCECSVSEPARATE := csvSeparator,
                    DESTINATIONGROUP := SprayClusterName,
                    DESTINATIONLOGICALNAME := sprayOutFileName,
                    TIMEOUT := -1,
                    ESPSERVERIPPORT := myRemoteEspFsURL,
                    ALLOWOVERWRITE := true
                    );
self.result := 'Spray Pass';
end;


dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := NOTHOR(PROJECT(NOFOLD(dst2), spray(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Spray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut := output(c2);
#else
    sprayOut := output(c2, {result});
#end


#if (mode[1..6] = 'single')
    ds := DATASET(sprayOutFileName, SingleFieldLayout, csv);

    string compareDatasets(dataset(SingleFieldLayout) ds1, dataset(SingleFieldLayout) ds2) := FUNCTION
        UNSIGNED c := COUNT(JOIN(ds1, ds2, left.field[0..5]=right.field[0..5], FULL ONLY));
        RETURN if( 0 = c, 'Pass', 'Fail (found ' + INTFORMAT(c,10,0) + ' records)');
    END;
#else
    ds := DATASET(sprayOutFileName, MultipleFieldLayout, csv);
    string compareDatasets(dataset(MultipleFieldLayout) ds1, dataset(MultipleFieldLayout) ds2) := FUNCTION
        UNSIGNED c := COUNT(JOIN(ds1, ds2, left.id=right.id, FULL ONLY));
        RETURN if( 0 = c, 'Pass', 'Fail (found ' + INTFORMAT(c,10,0) + ' records)');
    END;
#end


SEQUENTIAL(
    setupFile,
    desprayOut,
    sprayOut,
    output(compareDatasets(dsSetup,ds),NAMED('DatasetCompare')),

#if (CLEAN_UP = 1)
    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName),
    FileServices.DeleteLogicalFile(sprayPrepFileName),
    FileServices.DeleteLogicalFile(sprayOutFileName)
#end
);
