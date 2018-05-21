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

#option('layoutTranslation', true);

import ^ as root;
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
#option('forceRemoteRead', optRemoteRead);


#onwarning(4523, ignore);


IMPORT STD;
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

fname := prefix + 'remoteread';
fname_comp := prefix + 'remoteread_comp';
fname_index := prefix + 'remoteread_index';

rec := RECORD
 string10 fname;
 string10 lname;
 unsigned age;
END;

inds := DATASET([{'John', 'Smith', 42}, {'Bob', 'Brown', 29}, {'Samuel', 'Jackson', 58} ], rec, DISTRIBUTED);
i := INDEX(inds, { fname }, { lname, age }, fname_index);


rec_vf := RECORD(rec)
 string50 logicalFile{virtual(logicalfilename)};
 unsigned8 fpos{virtual(fileposition)};
// unsigned8 local_fpos{virtual(localfileposition)}; // NB: Don't include in test, because will vary per engine per file width written
END;

rec_vf_trans := RECORD
 string lname;
 string fname;
 unsigned8 fpos{virtual(fileposition)};
 string logicalFile{virtual(logicalfilename)};
END;

ds := DATASET(fname, rec_vf, FLAT);
ds_comp := DATASET(fname_comp, rec_vf, FLAT);
ds_trans := DATASET(fname, rec_vf_trans, FLAT);

SEQUENTIAL(
 OUTPUT(inds, , fname, OVERWRITE);
 OUTPUT(inds, , fname_comp, COMPRESSED, OVERWRITE);
 BUILDINDEX(i, OVERWRITE);

 OUTPUT(ds);
 OUTPUT(ds_comp);
 OUTPUT(i);
 OUTPUT(ds_trans);
);


