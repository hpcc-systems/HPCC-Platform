/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

//version optRemoteRead=false
//version optRemoteRead=true
//version optRemoteRead=true,optCompression='LZ4'

#option('layoutTranslation', true);

import Std.File AS FileServices;
import Std.System.Thorlib;
import ^ as root;
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
optCompression := #IFDEFINED(root.optCompression, '');
#option('forceRemoteRead', optRemoteRead);
#option('remoteCompressedOutput', optCompression);


#onwarning(4523, ignore);


IMPORT STD;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

//MORE: If executed in parallel the prefix could cause problems - but outputing the filenames causes mismatches
rrPrefix := IF(optRemoteRead, 'R', 'L');
cmPrefix := IF(optCompression!='', optCompression, 'X');
//xxprefix := filePrefix + rrPrefix + cmPrefix;

fname := prefix + 'remoteread';
fname_comp := prefix + 'remoteread_comp';
fname_index := prefix + 'remoteread_index';
fname_large := prefix + 'remoteread_large';
fname_large_out := prefix + 'remoteread_largeout';
fname_large_index := prefix + 'remoteread_large_index';

rec := RECORD
 string10 fname;
 string10 lname;
 unsigned age;
END;

inds := DATASET([{'John', 'Smith', 42}, {'Bob', 'Brown', 29}, {'Samuel', 'Jackson', 58} ], rec, DISTRIBUTED);
i := INDEX(inds, { fname }, { lname, age }, fname_index);


rec_vf := RECORD(rec)
 string logicalFile{virtual(logicalfilename)};
 unsigned8 fpos{virtual(fileposition)};
// unsigned8 local_fpos{virtual(localfileposition)}; // NB: Don't include in test, because will vary per engine per file width written
END;

rec_vf_trans := RECORD
 string lname;
 string fname;
 unsigned8 fpos{virtual(fileposition)};
 string logicalFile{virtual(logicalfilename)};
END;

string trimmedLogicalFilename(STRING filename) := FUNCTION
    return std.str.FindReplace(std.str.ToLowerCase(trim(filename)), setup.Files(false, false).QueryFilePrefixId, '');
end;

ds := project(DATASET(fname, rec_vf, FLAT), transform(rec_vf, SELF.logicalFile := trimmedLogicalFilename(LEFT.logicalFile); SELF := LEFT;));
ds_comp := project(DATASET(fname_comp, rec_vf, FLAT), transform(rec_vf, SELF.logicalFile := trimmedLogicalFilename(LEFT.logicalFile); SELF := LEFT;));
ds_trans := project(DATASET(fname, rec_vf_trans, FLAT), transform(rec_vf_trans, SELF.logicalFile := trimmedLogicalFilename(LEFT.logicalFile); SELF := LEFT;));

// big enough to test default buffer limits in dafilesrv
largeds := DATASET(1000000, TRANSFORM(rec, SELF.fname := TRIM(inds[(COUNTER%3)+1].fname)+(string)COUNTER; SELF.lname := TRIM(inds[(COUNTER%3)+1].lname)+(string)COUNTER; SELF.age := inds[(COUNTER%3)+1].age+COUNTER));
largeoutrec := RECORD // shuffles order
 unsigned age;
 string10 lname;
 string10 fname;
END;


dsLarge := DATASET(fname_large, rec, FLAT);
idxLarge := INDEX(dsLarge,,fname_large_index);


SEQUENTIAL(
 OUTPUT(inds, , fname, OVERWRITE);
 OUTPUT(inds, , fname_comp, COMPRESSED, OVERWRITE);
 BUILDINDEX(i, OVERWRITE);
 OUTPUT(largeds, , fname_large, OVERWRITE);
 OUTPUT(largeds, , fname_large, OVERWRITE);
 BUILD(idxLarge);
 OUTPUT(ds);
 OUTPUT(ds_comp);
 OUTPUT(i);
 OUTPUT(ds_trans);
 output(choosen(dsLarge(age=54), 5));
 output(choosen(dsLarge(keyed(age=54)), 5));
 output(choosen(idxLarge(age=54), 5));
 SUM(NOFOLD(dsLarge), age);

 // Clean-up
 FileServices.DeleteLogicalFile(fname),
 FileServices.DeleteLogicalFile(fname_comp),
 FileServices.DeleteLogicalFile(fname_index),
 FileServices.DeleteLogicalFile(fname_large),
 FileServices.DeleteLogicalFile(fname_large_index),
);


