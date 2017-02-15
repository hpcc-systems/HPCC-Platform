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

import $.setup;
Files := setup.Files(false, false, false);

// Test compile-time field translation for keyed joins

#option ('reportDFSinfo',2);
DG_IndexName := Files.DG_IndexOut+'INDEX';

Slim_OutRec := RECORD
    string10  DG_firstname;
    string10  DG_lastname;
END;

Extra_OutRec := RECORD
    string10  DG_firstname;
    string3  DG_lastname;
    string1   newfield { DEFAULT('Y')};
END;

'Full-keyed joins, RHS and LHS translated';

// Test no translation requested
no_transR := DATASET(Files.DG_ParentFileOut,{Files.DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,LOOKUP(FALSE));
no_transI := INDEX(no_transR, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName, LOOKUP(FALSE));

// Test no translation needed
no_trans_neededR := DATASET(Files.DG_ParentFileOut,{Files.DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,LOOKUP(TRUE));
no_trans_neededI := INDEX(no_trans_neededR, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName, LOOKUP(TRUE));

// Test removing some fields
slimmedR := DATASET(Files.DG_ParentFileOut,{Slim_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,LOOKUP(TRUE));
slimmedI := INDEX(slimmedR, { DG_firstname, DG_lastname }, { filepos }, DG_IndexName, LOOKUP(TRUE));

// Test adding/changing some fields
addedR := DATASET(Files.DG_ParentFileOut,{Extra_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT,LOOKUP(TRUE));
addedI := INDEX(addedR, { DG_firstname, DG_lastname }, { newfield, filepos }, DG_IndexName, LOOKUP(TRUE));

SEQUENTIAL(
  OUTPUT(JOIN(Files.DG_FlatFileEvens, no_transR, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(no_transI),KEEP(1)));
  OUTPUT(JOIN(Files.DG_FlatFileEvens, no_trans_neededR, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(no_trans_neededI),KEEP(1)));
  OUTPUT(JOIN(Files.DG_FlatFileEvens, slimmedR, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(slimmedI),KEEP(1)));
  OUTPUT(JOIN(Files.DG_FlatFileEvens, addedR, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(addedI),KEEP(1)));
);

