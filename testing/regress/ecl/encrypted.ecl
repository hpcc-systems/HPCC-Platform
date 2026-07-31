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

// As far as I can tell roxie has no support for reading from encrypted files.
// Treat as a separate issue
//noroxie

import Std.File;

filename := WORKUNIT + '_encrypted' : global;
rec := {unsigned id};

ds := DATASET(100, transform(rec, SELF.id := COUNTER), DISTRIBUTED);
doCreate() := OUTPUT(ds,,filename,ENCRYPT('WORD'), OVERWRITE);

ds2 := DATASET(filename, rec, THOR, ENCRYPT('WORD'));
s := SUM(NOFOLD(ds2), id);
doVerify() := output(s - 5050);

ordered(
    doCreate(),
    doVerify(),
    File.DeleteLogicalFile(filename,true)
);

