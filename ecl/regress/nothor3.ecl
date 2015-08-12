/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

import lib_fileservices;

/*
//Simple output from nothor
nothor(output(FileServices.LogicalFileList('*', true, false)));

//Same, but extending an output
nothor(output(FileServices.LogicalFileList('regress::hthor::simple*', true, false),named('Out2'),extend));
nothor(output(FileServices.LogicalFileList('regress::hthor::house*', true, false),named('Out2'),extend));
*/

//Same, but can't do on own
ds := FileServices.LogicalFileList('*', true, false);
summary := table(nothor(ds), { owner, numFiles := count(group), totalSize := sum(group, size) }, owner);

output(summary);

