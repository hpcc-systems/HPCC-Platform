/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

import Std.File AS FileServices;

// This is not an engine test, but a DFU.
// Doesn't matter much which engine does it, so we restrict to only one

//noRoxie
//noThorLCR
//noThor

string OriginalTextFilesIp := '.' : STORED('OriginalTextFilesIp');
string OriginalTextFilesEclPath := '' : STORED('OriginalTextFilesEclPath');
string OriginalTextFilesOsPath := '' : STORED('OriginalTextFilesOsPath');

File := OriginalTextFilesOsPath + '/download/spray_test.txt';
RecordSize := 11;
ClusterName := 'mythor';
DestFile := '~regress::spray_test.txt';
Timeout := -1;
ESPportIP := 'http://127.0.0.1:8010/FileSpray';

FileServices.SprayFixed(OriginalTextFilesIp,
                        File,
                        RecordSize,
                        ClusterName,
                        DestFile,
                        Timeout,
                        ESPportIP,
                        ALLOWOVERWRITE := true);

ds := DATASET(DestFile, { string name, unsigned age, boolean good }, csv);
output(ds);
