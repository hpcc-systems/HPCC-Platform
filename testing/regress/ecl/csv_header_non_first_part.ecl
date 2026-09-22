/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems.

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

// Overview:
// - Tests CSV header handling when a logical file has leading empty parts.
//
// Process:
// - Create an empty CSV and a headed CSV in the drop zone.
// - Spray six one-part empty files and one two-part headed file.
// - Combine them into a superfile with empty parts 1-6 and the header in part 7.
// - Copy the superfile with asSuperfile=FALSE to create a regular logical file (csv_with_header_single).
// - The regular logical file (csv_with_header_single) preserves the leading empty parts and has its header in part 7.
// - Output SuperRowCount and SingleRowCount
// - Verify that the regular file (csv_with_header_single) dataset doesn't contain the header file.
//
// Expected results:
// - SuperRowCount and SingleRowCount are both 1000.
// - SingleHeaderLeaked is FALSE.
// - Before the fix, the header in part 7 was returned as data and SingleRowCount was 1001.
//
IMPORT Std;
IMPORT Std.System.Thorlib;
IMPORT $.setup;

//class=superfile
//class=slow

prefix := setup.Files(false, false).QueryFilePrefix;
dropzonePlane := Std.File.GetDefaultDropZoneName();
sprayDestGroup := Thorlib.group();

emptyStage := prefix + 'csv_part_empty_stage';
headerStage := prefix + 'csv_part_header_stage';
emptyPath := WORKUNIT + '-csv_part-empty.csv';
headerPath := WORKUNIT + '-csv_part-header.csv';

empty1 := prefix + 'csv_part_empty1';
empty2 := prefix + 'csv_part_empty2';
empty3 := prefix + 'csv_part_empty3';
empty4 := prefix + 'csv_part_empty4';
empty5 := prefix + 'csv_part_empty5';
empty6 := prefix + 'csv_part_empty6';
headed := prefix + 'csv_part_headed';
superName := prefix + 'csv_with_header_super';
singleName := prefix + 'csv_with_header_single';

CsvRec := RECORD
    STRING id { MAXLENGTH(10) };
    STRING name { MAXLENGTH(50) };
END;

emptyDs := DATASET([], CsvRec);
headedDs := DATASET(1000, TRANSFORM(CsvRec,
    SELF.id := (STRING)COUNTER,
    SELF.name := 'Employee ' + (STRING)COUNTER
));

DeleteExistingFiles := SEQUENTIAL(
    Std.File.DeleteSuperFile(superName),
    Std.File.DeleteLogicalFile(emptyStage),
    Std.File.DeleteLogicalFile(headerStage),
    Std.File.DeleteLogicalFile(empty1),
    Std.File.DeleteLogicalFile(empty2),
    Std.File.DeleteLogicalFile(empty3),
    Std.File.DeleteLogicalFile(empty4),
    Std.File.DeleteLogicalFile(empty5),
    Std.File.DeleteLogicalFile(empty6),
    Std.File.DeleteLogicalFile(headed),
    Std.File.DeleteLogicalFile(singleName),
    Std.File.DeleteExternalFile('.', emptyPath, dropzonePlane),
    Std.File.DeleteExternalFile('.', headerPath, dropzonePlane)
);

CreateSourceFiles := SEQUENTIAL(
    OUTPUT(emptyDs, , emptyStage, OVERWRITE, CSV),
    OUTPUT(headedDs, , headerStage, OVERWRITE, CSV(HEADING(SINGLE), QUOTE('"'))),
    Std.File.Despray(emptyStage, DESTINATIONPATH := emptyPath, DESTINATIONPLANE := dropzonePlane, ALLOWOVERWRITE := TRUE),
    Std.File.Despray(headerStage, DESTINATIONPATH := headerPath, DESTINATIONPLANE := dropzonePlane, ALLOWOVERWRITE := TRUE)
);

SpraySevenOnePartFiles := SEQUENTIAL(
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty1,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty2,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty3,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty4,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty5,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := emptyPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := empty6,
        DESTINATIONNUMPARTS := 1,
        ALLOWOVERWRITE := TRUE
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := headerPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := headed,
        DESTINATIONNUMPARTS := 2,
        ALLOWOVERWRITE := TRUE
    )
);

BuildSuperFile := SEQUENTIAL(
    Std.File.CreateSuperFile(superName),
    Std.File.StartSuperFileTransaction(),
    Std.File.AddSuperFile(superName, empty1),
    Std.File.AddSuperFile(superName, empty2),
    Std.File.AddSuperFile(superName, empty3),
    Std.File.AddSuperFile(superName, empty4),
    Std.File.AddSuperFile(superName, empty5),
    Std.File.AddSuperFile(superName, empty6),
    Std.File.AddSuperFile(superName, headed),
    Std.File.FinishSuperFileTransaction()
);

CopyAsSingleFile := Std.File.Copy(
    sourceLogicalName := superName,
    destinationGroup := sprayDestGroup,
    destinationLogicalName := singleName,
    allowOverwrite := TRUE,
    asSuperfile := FALSE,
    noSplit := TRUE
);

SuperRows := DATASET(superName, CsvRec, CSV(HEADING(1)));
SingleRows := DATASET(singleName, CsvRec, CSV(HEADING(1)));

VerifyLayout := SEQUENTIAL(
    OUTPUT(COUNT(SuperRows), NAMED('SuperRowCount')),
    OUTPUT(COUNT(SingleRows), NAMED('SingleRowCount')),
    OUTPUT(EXISTS(SingleRows(name = 'name')), NAMED('SingleHeaderLeaked'))
);

SEQUENTIAL(
    CreateSourceFiles,
    SpraySevenOnePartFiles,
    BuildSuperFile,
    CopyAsSingleFile,
    VerifyLayout,
    DeleteExistingFiles
);
