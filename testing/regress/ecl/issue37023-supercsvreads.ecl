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

IMPORT STD;
import $.setup;

//noroxie

// Read a superfile of CSV logical files with and without HEADING(SINGLE)
// and compare counts across hthor and Thor clusters of different sizes.

prefix := setup.Files(false, false).QueryFilePrefix;
logical1 := prefix + 'tinytimezones1';
logical2 := prefix + 'tinytimezones2';
superName := prefix + 'tinytimezonesuper';

rOut := RECORD
    STRING16 entryid;
    STRING32 state_region;
    STRING64 timezone;
END;

// 19 data rows per file; each file will also contain 1 CSV header row.
ds1 := DATASET([
    {'1', 'AL', 'America/Chicago'},
    {'2', 'AK', 'America/Anchorage'},
    {'3', 'AZ', 'America/Phoenix'},
    {'4', 'AR', 'America/Chicago'},
    {'5', 'CA', 'America/Los_Angeles'},
    {'6', 'CO', 'America/Denver'},
    {'7', 'CT', 'America/New_York'},
    {'8', 'DE', 'America/New_York'},
    {'9', 'FL', 'America/New_York'},
    {'10', 'GA', 'America/New_York'},
    {'11', 'HI', 'Pacific/Honolulu'},
    {'12', 'IA', 'America/Chicago'},
    {'13', 'ID', 'America/Boise'},
    {'14', 'IL', 'America/Chicago'},
    {'15', 'IN', 'America/Indiana/Indianapolis'},
    {'16', 'KS', 'America/Chicago'},
    {'17', 'KY', 'America/New_York'},
    {'18', 'LA', 'America/Chicago'},
    {'19', 'MA', 'America/New_York'}
], rOut);

ds2 := DATASET([
    {'20', 'MD', 'America/New_York'},
    {'21', 'ME', 'America/New_York'},
    {'22', 'MI', 'America/Detroit'},
    {'23', 'MN', 'America/Chicago'},
    {'24', 'MO', 'America/Chicago'},
    {'25', 'MS', 'America/Chicago'},
    {'26', 'MT', 'America/Denver'},
    {'27', 'NC', 'America/New_York'},
    {'28', 'ND', 'America/Chicago'},
    {'29', 'NE', 'America/Chicago'},
    {'30', 'NH', 'America/New_York'},
    {'31', 'NJ', 'America/New_York'},
    {'32', 'NM', 'America/Denver'},
    {'33', 'NV', 'America/Los_Angeles'},
    {'34', 'NY', 'America/New_York'},
    {'35', 'OH', 'America/New_York'},
    {'36', 'OK', 'America/Chicago'},
    {'37', 'OR', 'America/Los_Angeles'},
    {'38', 'PA', 'America/New_York'}
], rOut);

createInputs := SEQUENTIAL(
    OUTPUT(ds1,, logical1, CSV(HEADING('entryid,state_region,timezone', SINGLE), QUOTE('"')), OVERWRITE),
    OUTPUT(ds2,, logical2, CSV(HEADING('entryid,state_region,timezone', SINGLE), QUOTE('"')), OVERWRITE)
);

buildSuper := SEQUENTIAL(
    STD.File.DeleteSuperFile(superName),
    STD.File.CreateSuperFile(superName),
    STD.File.StartSuperFileTransaction(),
    STD.File.AddSuperFile(superName, logical1),
    STD.File.AddSuperFile(superName, logical2),
    STD.File.FinishSuperFileTransaction()
);

ds := DATASET(superName, rOut, CSV);
dsSingle := DATASET(superName, rOut, CSV(HEADING(SINGLE)));

SEQUENTIAL(
    createInputs,
    buildSuper,
    OUTPUT(COUNT(ds), NAMED('count_without_heading_single')),
    OUTPUT(COUNT(dsSingle), NAMED('count_with_heading_single')),
    OUTPUT(ds, ALL, NAMED('rows_without_heading_single')),
    OUTPUT(dsSingle, ALL, NAMED('rows_with_heading_single')),
    STD.File.DeleteSuperFile(superName),
    STD.File.DeleteLogicalFile(logical1),
    STD.File.DeleteLogicalFile(logical2)
);

