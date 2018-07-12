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
    string3   DG_lastname;
    string1   newfield { DEFAULT('Y')};
END;

'Half-keyed joins';

// Test no translation requested
no_trans := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName);
OUTPUT(JOIN(Files.DG_FlatFile, no_trans, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEEP(1)));

// Test no translation needed
no_trans_needed := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, filepos }, DG_IndexName, LOOKUP(TRUE));
OUTPUT(JOIN(Files.DG_FlatFile, no_trans_needed, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEEP(1)));

// Test removing some fields
slimmed := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, DG_IndexName, LOOKUP(TRUE));
OUTPUT(JOIN(Files.DG_FlatFile, slimmed, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEEP(1)));

// Test adding/changing some fields
added := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, string1 newfield {default('Y')}, filepos}, DG_IndexName, LOOKUP(TRUE));
OUTPUT(JOIN(Files.DG_FlatFile, added, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEEP(1)));

// Test OPT
notthere := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname }, { DG_Prange, string1 newfield {default('Y')}, filepos}, DG_IndexName+'missing', LOOKUP(TRUE), OPT);
OUTPUT(JOIN(Files.DG_FlatFile, notthere, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEEP(1))[1]);

'Full keyed joins';

// Test no translation requested
OUTPUT(JOIN(Files.DG_FlatFileEvens, Files.DG_FlatFile, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(no_trans),KEEP(1)));

// Test no translation needed
OUTPUT(JOIN(Files.DG_FlatFileEvens, Files.DG_FlatFile, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(no_trans_needed),KEEP(1)));

// Test removing some fields
slimmed2 := INDEX(Files.DG_FlatFile, { DG_firstname, DG_lastname, unsigned dummyFpos := 0  }, DG_IndexName, LOOKUP(TRUE));
OUTPUT(JOIN(Files.DG_FlatFileEvens, Files.DG_FlatFile, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(slimmed2),KEEP(1)));

// Test adding/changing some fields
OUTPUT(JOIN(Files.DG_FlatFileEvens, Files.DG_FlatFile, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(added),KEEP(1)));

// Test OPT
OUTPUT(JOIN(Files.DG_FlatFileEvens, Files.DG_FlatFile, KEYED(LEFT.DG_Firstname = RIGHT.DG_firstname),KEYED(notthere),KEEP(1)));
