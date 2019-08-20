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

//class=file
//class=index
//class=error
//fail

import ^ as root;
import $.setup;
Files := setup.Files(false, false, true);

i := INDEX(Files.DG_FlatFile, { DG_lastname, DG_firstname }, { DG_Prange, filepos }, Files.DG_IndexOut+'INDEX');
o := i(DG_firstname = 'KIMBERLY', DG_LASTNAME='SMITH');

OUTPUT(i);
