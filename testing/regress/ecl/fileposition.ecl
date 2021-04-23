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

//version multiPart=false
//xversion multiPart=true           // cannot use this because results depend on the number of thor slaves
//noroxie      - see HPCC-22629

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);

import $.setup;
import Std.System.ThorLib;
Files := setup.Files(multiPart, false);

// Roxie needs this to resolve files at run time
#option ('allowVariableRoxieFilenames', 1);
#option('forceRemoteRead', optRemoteRead);

inDs := Files.seqDupFile;

sequential(
    output(inDs, { seq, filepos, unsigned part := (localfilepos - 0x8000000000000000) >> 48,  unsigned offset := (localfilepos & 0xFFFFFFFFFFFF) })
);
