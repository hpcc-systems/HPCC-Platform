/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//UseStandardFiles
//UseIndexes
//varskip payload
//varskip varload
//varskip trans
//nothor

//Stepped global joins unsupported, see issue HPCC-8148
//skip type==thorlcr TBD

// should be equivalent to OUTPUT(SORT(DG_IndexFile(DG_firstname = 'DAVID'), DG_Prange));
OUTPUT(STEPPED(DG_IndexFile(KEYED(DG_firstname = 'DAVID')), DG_Prange));
