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

// Test compile-time field translation - recordof variant - for indexes

// Test the LOOKUP attribute on recordof

#option ('reportDFSinfo',2);
DG_IndexName := Files.DG_IndexOut+'INDEX';

r := RECORDOF(Files.DG_FlatFile);
// Test no translation requested
no_trans_requested_record := RECORDOF(DG_IndexName, { r.DG_firstname, r.DG_lastname => r.DG_Prange, r.filepos }, LOOKUP(FALSE));
no_trans := INDEX(Files.DG_FlatFile, no_trans_requested_record, DG_IndexName);
choosen(no_trans(KEYED(DG_firstname = 'DAVID')),10);

// Test no translation needed
no_trans_needed_record := RECORDOF(DG_IndexName, { r.DG_firstname, r.DG_lastname => r.DG_Prange, r.filepos }, LOOKUP(TRUE));
no_trans_needed := INDEX(Files.DG_FlatFile, no_trans_needed_record, DG_IndexName);
choosen(no_trans_needed(KEYED(DG_firstname = 'DAVID')),10);

// Test removing some fields
slimmed_record := RECORDOF(DG_IndexName, { r.DG_firstname, r.DG_lastname }, LOOKUP(TRUE));
slimmed := INDEX(Files.DG_FlatFile, slimmed_record, DG_IndexName);
choosen(slimmed(KEYED(DG_firstname = 'DAVID')),10);

// Test changing fields (adding is not allowed)
added_record := RECORDOF(DG_IndexName, { r.DG_firstname, STRING3 DG_lastname => r.DG_Prange, r.filepos }, LOOKUP(TRUE));
added := INDEX(Files.DG_FlatFile, added_record, DG_IndexName);
choosen(added(KEYED(DG_firstname = 'DAVID')),10);

// Test OPT
notthere_record := RECORDOF(DG_IndexName+'missing', { r.DG_firstname, r.DG_lastname => r.DG_Prange, string1 newfield {default('Y')}, r.filepos }, LOOKUP, OPT);
notthere := INDEX(Files.DG_FlatFile, added_record, DG_IndexName+'missing', OPT);
output(choosen(notthere,1)[1]);
