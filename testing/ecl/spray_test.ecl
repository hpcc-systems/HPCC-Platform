/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

import Std.File AS FileServices;

// This is not an engine test, but a DFU.
// Doesn't matter much which engine does it, so we restrict to only one

//noRoxie
//noThorLCR
//noThor

SrcIP := 'localhost';
File := setupTextFileLocation + '/spray_test.txt';
RecordSize := 11;
ClusterName := 'mythor';
DestFile := '~regress::spray_test.txt';
Timeout := -1;
ESPportIP := 'http://127.0.0.1:8010/FileSpray';

FileServices.SprayFixed(SrcIP,
                        File,
                        RecordSize,
                        ClusterName,
                        DestFile,
                        Timeout,
                        ESPportIP,
                        ALLOWOVERWRITE := true);

ds := DATASET(DestFile, { string name, unsigned age, boolean good }, csv);
output(ds);
