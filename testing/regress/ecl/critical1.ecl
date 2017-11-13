/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
###############################################################################*/
import Std;
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

//noroxie
//nohthor

rawRecord := RECORD
    unsigned key;
END;

taggedRecord := RECORD(rawRecord)
    unsigned8 id;
END;

idFileName := prefix+'ids';

// Create superfile and initialize with first id number
InitializeSuperFile := FUNCTION
    InitialDS := DATASET([{0,1}],taggedRecord);
    initialIdFileName := idFilename + '_initial';
    newDS:=OUTPUT(InitialDS,,initialIdFileName,THOR,OVERWRITE);

   return IF(not Std.File.SuperFileExists(idFileName),
             SEQUENTIAL(Std.File.CreateSuperFile(idFileName, false, true),newDS, Std.File.AddSuperFile(idFileName, initialIdFileName)) );
END;

// Create new file with keys not already in current file.
processInput(dataset(rawRecord) inFile, string outFileName) := FUNCTION
    existingIds := DATASET(idFileName, taggedRecord, THOR);
    unmatchedKeys := JOIN(inFile, existingIds, LEFT.key = RIGHT.key, LEFT ONLY, LOOKUP);
    maxId := MAX(existingIds, id);
    newIdFileName := idFileName + WORKUNIT;
    newIds := PROJECT(unmatchedKeys, TRANSFORM(taggedRecord, SELF.id := maxId + COUNTER; SELF := LEFT));
    tagged := JOIN(inFile, existingIds + newIds, LEFT.key = RIGHT.key, TRANSFORM(taggedRecord, SELF.id := RIGHT.id, SELF := LEFT),lookup);

    updateIds := OUTPUT(newIds,,newIdFileName);
    extendSuper := Std.File.AddSuperFile(idFileName, newIdFileName);

    result := SEQUENTIAL(InitializeSuperFile, updateIds, extendSuper): CRITICAL('critical_test');

    RETURN result;
END;

/* Generate sample input data */
inputDataset := DATASET(1000000, TRANSFORM(rawRecord, SELF.key := RANDOM() % 1000000));
datasetOutsideCritical := inputDataset : independent;

/* Post process check - all id's should be unique*/
sortedds := SORT(DATASET(idFileName,taggedRecord,THOR), id);
dedupds := DEDUP(sortedds, left.id=right.id);
dedupds_cnt := COUNT(dedupds);
sortedds_cnt := COUNT(sortedds);

SEQUENTIAL(
    EVALUATE(datasetOutsideCritical),
    processInput(datasetOutsideCritical, prefix+'tagged'),
    OUTPUT(sortedds_cnt-dedupds_cnt,NAMED('DuplicateIdCount') ), // This should be zero
    Std.File.DeleteSuperFile(idFileName)
);
