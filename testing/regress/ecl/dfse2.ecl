/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//class=file
//class=error
//fail

import $.setup;
Files := setup.Files(false, false, false);

// Test compile-time field translation for indexes - error cases

#option ('reportDFSinfo',2);
DG_IndexName := Files.DG_IndexOut+'INDEX';

// layout change causes KEYED to fail

slimmed := INDEX(Files.DG_FlatFile, { DG_lastname }, DG_IndexName, LOOKUP(TRUE));
choosen(slimmed(KEYED(DG_lastname = 'SMITH')),10);   // Not keyable once translated...

