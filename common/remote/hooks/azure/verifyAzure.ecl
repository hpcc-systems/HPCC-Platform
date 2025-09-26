/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#OPTION('pickBestEngine', false);
#OPTION('avoidRename', true);
unsigned8 mib := 100;
unsigned8 totalrecs := mib*1024*1024/5;
rec := {UNSIGNED5 num};


outdata := DATASET(totalrecs,TRANSFORM(rec,SELF.num:=COUNTER),DISTRIBUTED);

// Azure blob API
OUTPUT(outdata,,'~testing::somescope::otherscope::blob',PLANE('data-blob'),OVERWRITE,NAMED('BLOB_OUTPUT'));
inblob := DATASET('~testing::somescope::otherscope::blob',rec,FLAT);
blob := COMBINE(inblob, outdata, transform({ boolean same, RECORDOF(LEFT) l, RECORDOF(RIGHT) r,  }, SELF.same := LEFT = RIGHT; SELF.l := LEFT; SELF.r := RIGHT ), LOCAL);
OUTPUT(CHOOSEN(blob(not same), 10),NAMED('BLOB_PASS_IF_0_ROWS'));  // should be empty

// Azure file share API
OUTPUT(outdata,,'~testing::somescope::otherscope::files',PLANE('data-files'),OVERWRITE,NAMED('FILES_OUTPUT'));
infile := DATASET('~testing::somescope::otherscope::files',rec,FLAT);
file := COMBINE(infile, outdata, transform({ boolean same, RECORDOF(LEFT) l, RECORDOF(RIGHT) r,  }, SELF.same := LEFT = RIGHT; SELF.l := LEFT; SELF.r := RIGHT ), LOCAL);
OUTPUT(CHOOSEN(file(not same), 10),NAMED('FILES_PASS_IF_0_ROWS'));  // should be empty
