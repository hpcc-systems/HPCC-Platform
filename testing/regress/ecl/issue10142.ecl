/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//noroxie

IMPORT STD;
sf1 := '~data::test::sf';
path1 := '~data::name1';
path2 := '~data::name2';

nRecord := RECORD
    STRING20 name;
END;
ds1 := DATASET([
        {'aaa'},
        {'bbb'},
        {'ccc'}
    ],
    nRecord);

ds2 := DATASET([
        {'ddd'}
    ], nRecord);

IF(STD.File.SuperFileExists(sf1), STD.File.ClearSuperFile(sf1), STD.File.CreateSuperFile(sf1));

ds3 := DATASET(sf1, {nRecord, string255 logicalFile{virtual(logicalfilename)}}, THOR);
ds4 := DATASET(path1, {nRecord, string255 logicalFile{virtual(logicalfilename)}}, THOR);

SEQUENTIAL(
    // create
    OUTPUT(ds1,,path1, OVERWRITE),
    OUTPUT(ds2,,path2, OVERWRITE),
    STD.File.StartSuperFileTransaction(),
    STD.File.AddSuperFile(sf1, path1),
    STD.File.AddSuperFile(sf1, path2),
    STD.File.FinishSuperFileTransaction(),

    OUTPUT(ds3),
    OUTPUT(ds4),

    // clean-up
    STD.File.StartSuperFileTransaction(),
    STD.File.RemoveSuperFile(sf1, path2),
    STD.File.RemoveSuperFile(sf1, path1),
    STD.File.FinishSuperFileTransaction(),
    STD.File.DeleteSuperFile(sf1)
    );
