/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

IMPORT Std;
IMPORT Std.System.Thorlib;
IMPORT $.setup;

//class=superfile
//nohthor
//noroxie
// This regression is only effective on clusters with width >= 4.
// On clusters with width < 4, it can pass even when the defect is still present,
// so it is not a reliable test there.
//class=need4workers

prefix := setup.Files(false, false).QueryFilePrefix;

createEngLogical := prefix + 'csv_header_eng_data';
createSalesLogical := prefix + 'csv_header_sales_data';
createOpsLogical := prefix + 'csv_header_ops_data';
createEmployeesSuperLogical := prefix + 'csv_header_all_employees_super';

stagedEngLogical := prefix + 'csv_header_eng_data_staged';
stagedSalesLogical := prefix + 'csv_header_sales_data_staged';
stagedOpsLogical := prefix + 'csv_header_ops_data_staged';

sprayDestGroup := Thorlib.group();
sprayNumParts := IF(CLUSTERSIZE > 1, CLUSTERSIZE DIV 2, 1);
dropzonePlane := Std.File.GetDefaultDropZoneName();
desprayPathBase := WORKUNIT + '-csv_header_superfile-';
desprayedEngPath := desprayPathBase + 'eng.csv';
desprayedSalesPath := desprayPathBase + 'sales.csv';
desprayedOpsPath := desprayPathBase + 'ops.csv';

inputEngLogical := createEngLogical;
inputSalesLogical := createSalesLogical;
inputOpsLogical := createOpsLogical;
inputEmployeesSuperLogical := createEmployeesSuperLogical;

EmployeeRec := RECORD
    STRING id { MAXLENGTH(5) };
    STRING name { MAXLENGTH(100) };
    INTEGER age;
    STRING department { MAXLENGTH(50) };
END;

seedNames := ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack',
             'Karen', 'Leo', 'Megan', 'Nathan', 'Olivia', 'Peter', 'Quinn', 'Rachel', 'Steve', 'Tina',
             'Uma', 'Victor', 'Wendy', 'Xavier', 'Yara', 'Zoe', 'Albert', 'Barbara', 'Carl', 'Donna'];

engCreateDs := DATASET(100, TRANSFORM(EmployeeRec,
    SELF.id := TRIM((STRING)(COUNTER + 100000))[2..],
    SELF.name := seedNames[(COUNTER-1) % 30 + 1] + ' E' + TRIM((STRING)(COUNTER + 100))[2..],
    SELF.age := 20 + (COUNTER * 7) % 46,
    SELF.department := 'Engineering'
));

salesCreateDs := DATASET(100, TRANSFORM(EmployeeRec,
    SELF.id := TRIM((STRING)(100 + COUNTER + 100000))[2..],
    SELF.name := seedNames[(COUNTER-1) % 30 + 1] + ' S' + TRIM((STRING)(COUNTER + 100))[2..],
    SELF.age := 21 + (COUNTER * 11) % 45,
    SELF.department := 'Sales'
));

opsCreateDs := DATASET(100, TRANSFORM(EmployeeRec,
    SELF.id := TRIM((STRING)(20000 + COUNTER + 100000))[2..],
    SELF.name := seedNames[(COUNTER-1) % 30 + 1] + ' O' + TRIM((STRING)(COUNTER + 100))[2..],
    SELF.age := 22 + (COUNTER * 13) % 44,
    SELF.department := 'Operations'
));

CreateLogicalFiles := SEQUENTIAL(
    Std.File.DeleteSuperFile(createEmployeesSuperLogical),
    Std.File.DeleteLogicalFile(stagedEngLogical),
    Std.File.DeleteLogicalFile(stagedSalesLogical),
    Std.File.DeleteLogicalFile(stagedOpsLogical),
    Std.File.DeleteLogicalFile(createEngLogical),
    Std.File.DeleteLogicalFile(createSalesLogical),
    Std.File.DeleteLogicalFile(createOpsLogical),

    OUTPUT(
        engCreateDs,
        , stagedEngLogical,
        OVERWRITE,
        CSV(HEADING(SINGLE), QUOTE('"'))
    ),
    OUTPUT(
        salesCreateDs,
        , stagedSalesLogical,
        OVERWRITE,
        CSV(HEADING(SINGLE), QUOTE('"'))
    ),
    OUTPUT(
        opsCreateDs,
        , stagedOpsLogical,
        OVERWRITE,
        CSV(HEADING(SINGLE), QUOTE('"'))
    ),

    // Despray staged single logical files to dropzone
    Std.File.Despray(stagedEngLogical, DESTINATIONPATH := desprayedEngPath, DESTINATIONPLANE := dropzonePlane, ALLOWOVERWRITE := TRUE),
    Std.File.Despray(stagedSalesLogical, DESTINATIONPATH := desprayedSalesPath, DESTINATIONPLANE := dropzonePlane, ALLOWOVERWRITE := TRUE),
    Std.File.Despray(stagedOpsLogical, DESTINATIONPATH := desprayedOpsPath, DESTINATIONPLANE := dropzonePlane, ALLOWOVERWRITE := TRUE),

    // Spray back with destinationNumParts = CLUSTERSIZE/2
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := desprayedEngPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := createEngLogical,
        ALLOWOVERWRITE := TRUE,
        DESTINATIONNUMPARTS := sprayNumParts
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := desprayedSalesPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := createSalesLogical,
        ALLOWOVERWRITE := TRUE,
        DESTINATIONNUMPARTS := sprayNumParts
    ),
    Std.File.SprayVariable(
        SOURCEPLANE := dropzonePlane,
        SOURCEPATH := desprayedOpsPath,
        DESTINATIONGROUP := sprayDestGroup,
        DESTINATIONLOGICALNAME := createOpsLogical,
        ALLOWOVERWRITE := TRUE,
        DESTINATIONNUMPARTS := sprayNumParts
    ),

    Std.File.CreateSuperFile(createEmployeesSuperLogical),
    Std.File.StartSuperFileTransaction(),
    Std.File.AddSuperFile(createEmployeesSuperLogical, createEngLogical),
    Std.File.AddSuperFile(createEmployeesSuperLogical, createSalesLogical),
    Std.File.AddSuperFile(createEmployeesSuperLogical, createOpsLogical),
    Std.File.FinishSuperFileTransaction()
);

readEngRows := DATASET(inputEngLogical, EmployeeRec, CSV(HEADING(1)));
readSalesRows := DATASET(inputSalesLogical, EmployeeRec, CSV(HEADING(1)));
readOpsRows := DATASET(inputOpsLogical, EmployeeRec, CSV(HEADING(1)));
readSuperRows := DATASET(inputEmployeesSuperLogical, EmployeeRec, CSV(HEADING(1)));

VerifyHeaderHandling := SEQUENTIAL(
    OUTPUT(
        COUNT(readEngRows),
        NAMED('CountEngData')
    ),

    OUTPUT(
        COUNT(readSalesRows),
        NAMED('CountSalesData')
    ),

    OUTPUT(
        COUNT(readOpsRows),
        NAMED('CountOpsData')
    ),

    OUTPUT(
        COUNT(readSuperRows),
        NAMED('TotalRowCount')
    ),

    OUTPUT(
        EXISTS(readSuperRows(name = 'name')),
        NAMED('HeaderLeaked')
    ),

    OUTPUT(
        TABLE(readSuperRows, { department, cnt := COUNT(GROUP) }, department, MERGE),
        NAMED('CountByDept')
    ),

    OUTPUT(
        COUNT(DEDUP(readSuperRows, name)),
        NAMED('DistinctNames')
    ),

    OUTPUT(
        readSuperRows,
        NAMED('AllEmployees')
    )
);

CleanupCreatedFiles := SEQUENTIAL(
    Std.File.DeleteSuperFile(createEmployeesSuperLogical),
    Std.File.DeleteLogicalFile(stagedEngLogical),
    Std.File.DeleteLogicalFile(stagedSalesLogical),
    Std.File.DeleteLogicalFile(stagedOpsLogical),
    Std.File.DeleteLogicalFile(createEngLogical),
    Std.File.DeleteLogicalFile(createSalesLogical),
    Std.File.DeleteLogicalFile(createOpsLogical),
    Std.File.DeleteExternalFile('.', desprayedEngPath, dropzonePlane),
    Std.File.DeleteExternalFile('.', desprayedSalesPath, dropzonePlane),
    Std.File.DeleteExternalFile('.', desprayedOpsPath, dropzonePlane)
);

CheckClusterWidth := IF(
    CLUSTERSIZE < 4,
    FAIL('Cluster width must be at least 4'),
    OUTPUT(CLUSTERSIZE, NAMED('ClusterSize'))
);

SEQUENTIAL(CheckClusterWidth, CreateLogicalFiles, VerifyHeaderHandling, CleanupCreatedFiles);
