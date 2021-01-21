/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#onwarning (4523, ignore);

prefix := setup.Files(false, false).QueryFilePrefix;

rec := RECORD
 string15 fname;
 string15 lname;
 unsigned age;
END;

inds1 := DATASET([{ 'Aaron', 'Jones', 100}, {'Adam', 'Smith', 90}, {'Bob', 'Brown', 80}, {'Brian', 'Brown', 70 }, {'Charles', 'Dance', 60}, {'Christopher', 'Gould', 50},  {'David', 'Brokenshire', 40}, {'Edward', 'Green', 30}, {'Egbert', 'Sillyname', 20}, {'Freddy', 'Peters', 10} ], rec, DISTRIBUTED);
inds2 := DATASET(10, TRANSFORM(rec, SELF.fname := (string)COUNTER; SELF.lname := (string)HASH(COUNTER); SELF.age := 1000-COUNTER));

indexName := prefix + 'myindex';
i := INDEX(inds1, { fname }, { lname, age }, indexName);
i2 := INDEX(inds2, { fname }, { lname, age }, indexName);

ds1 := prefix + 'ds1';
ds2 := prefix + 'ds2';

SEQUENTIAL(
    OUTPUT(inds1, , ds1, OVERWRITE);
    OUTPUT(inds2, , ds2, OVERWRITE);
    BUILDINDEX(i, SORTED, OVERWRITE);
    OUTPUT(i);
    BUILDINDEX(i2, OVERWRITE);
    OUTPUT(i2);
 
    // Clean-up
    Std.File.DeleteLogicalFile(indexName);
    Std.File.DeleteLogicalFile(ds1);
    Std.File.DeleteLogicalFile(ds2);
);
