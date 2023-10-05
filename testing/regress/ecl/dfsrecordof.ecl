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

// Test compile-time field translation - recordof variant

Slim_OutRec := RECORD
    string10  DG_firstname;
    string10  DG_lastname;
END;

Extra_OutRec := RECORD
    string10  DG_firstname;
    string3   DG_lastname;
END;

// Test the LOOKUP attribute on recordof

#option ('reportDFSinfo',2);

// Test no translation requested
no_trans_requested_record := RECORDOF(Files.DG_ParentFileOut, Files.DG_OutRec, LOOKUP(FALSE));
no_trans := DATASET(Files.DG_ParentFileOut,no_trans_requested_record,FLAT);
output(no_trans);

// Test no translation needed
no_trans_record := RECORDOF(Files.DG_ParentFileOut, Files.DG_OutRec, LOOKUP);
no_trans_needed := DATASET(Files.DG_ParentFileOut,no_trans_record,FLAT);
output(no_trans_needed);

// Test removing some fields
slimmed_record := RECORDOF(Files.DG_ParentFileOut, Slim_OutRec, LOOKUP);
slimmed := DATASET(Files.DG_ParentFileOut,slimmed_record,FLAT);
output(slimmed);

// changing fields (adding is not allowed)
changed_record := RECORDOF(Files.DG_ParentFileOut, Extra_OutRec, LOOKUP(TRUE));
changed := DATASET(Files.DG_ParentFileOut,changed_record,FLAT);
output(changed);

// Test OPT
notthere_record := RECORDOF(Files.DG_ParentFileOut, Extra_OutRec, LOOKUP, OPT);
notthere := DATASET(Files.DG_ParentFileOut+'missing',notthere_record,FLAT,OPT);
output(notthere[1]);

