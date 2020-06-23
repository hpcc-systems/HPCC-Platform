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

// Analysis should show: large number of rows read and rejected for keyed join
//

//nokey
//noroxie
//nohthor
//nooutput

IMPORT $.common.Files as Files;
IMPORT $.common.Helper as Helper;

j := JOIN(Files.dsfile1, Files.INDX1, KEYED(LEFT.user = 'Ned' and LEFT.user = RIGHT.firstname), ATMOST(1));

O := Output(j,,Files.joinresultds, OVERWRITE);
PARALLEL(O, Helper.saveWUID('anakjexcessrejects'));
