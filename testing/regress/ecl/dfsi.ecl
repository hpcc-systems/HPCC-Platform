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

#onwarning(7103, ignore);

import $.setup;
Files := setup.Files(false, false, false);

// Test compile-time field translation for indexes

#option ('reportDFSinfo',2);
DG_IndexName := Files.DG_IndexOut+'INDEX';

// Test no translation requested
no_trans := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName);
choosen(no_trans(KEYED(DG_firstname = 'DAVID')),10);

// Test no translation needed
no_trans_needed := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName, LOOKUP(TRUE));
choosen(no_trans_needed(KEYED(DG_firstname = 'DAVID')),10);

// Test removing some fields
slimmed := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, DG_IndexName, LOOKUP(TRUE));
choosen(slimmed(KEYED(DG_firstname = 'DAVID')),10);

// Test adding/changing some fields
added := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, string1 newfield {default('Y')}, filepos}, DG_IndexName, LOOKUP(TRUE));
choosen(added(KEYED(DG_firstname = 'DAVID')),10);

// Test OPT
notthere := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, string1 newfield {default('Y')}, filepos}, DG_IndexName+'missing', LOOKUP, OPT);
output(choosen(notthere,1)[1]);
