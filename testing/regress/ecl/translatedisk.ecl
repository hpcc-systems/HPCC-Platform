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

//version multiPart=false
//version multiPart=true
//version multiPart=true,optRemoteRead=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
optRemoteRead := #IFDEFINED(root.optRemoteRead, true);

//--- end of version configuration ---

#onwarning(2036, ignore);
#onwarning(4522, ignore);
#option ('layoutTranslation', 'payload');
#option('forceRemoteRead', optRemoteRead);
import $.Setup;

boolean useLocal := false;
Files := Setup.Files(multiPart, useLocal);

DG_FlatFile := PRELOAD(DATASET(Files.DG_FileOut+'FLAT',{Files.DG_OutRec.DG_LastName, Files.DG_OutRec, UNSIGNED8 filepos{virtual(fileposition)}},FLAT, HINT(key('''
<Keys>
 <MemIndex>
  <FieldSet>
   <Field name='DG_LastName'/>
  </FieldSet>
 </MemIndex>
</Keys>
'''))));
output(DG_FlatFile(KEYED(DG_lastname = 'DOLSON')));  // Should use in-memory key
output(DG_FlatFile(KEYED(DG_firstname = 'DAVID')));  // Should use in-memory, but no key
OUTPUT('----');

DG_FlatFile_add1 := PRELOAD(DATASET(Files.DG_FileOut+'FLAT',{Files.DG_OutRec,STRING newfield { default('new')}, UNSIGNED8 filepos{virtual(fileposition)}},FLAT));
output(DG_FlatFile_add1);
OUTPUT('----');

newsub := RECORD
  string2 newsub { default(':)') };
END;

DG_FlatFile_add2 := PRELOAD(DATASET(Files.DG_FileOut+'FLAT',{Files.DG_OutRec, STRING3 newfield { default('NEW')}, DATASET(newsub) sub, UNSIGNED8 filepos{virtual(fileposition)}},FLAT));
output(DG_FlatFile_add2);

d := table(DG_FlatFile, {unsigned8 fpos := DG_FlatFile.filepos} );
output(FETCH(DG_FlatFile_add2, d, right.fpos, TRANSFORM (RECORDOF(DG_FlatFile_add2), SELF.filepos := right.fpos; SELF := LEFT)));

DG_FlatFile_add3 := DATASET(Files.DG_FileOut+'FLAT',{Files.DG_OutRec,STRING newfield { default('Translated Full Keyed')}, UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
fkj :=JOIN(Files.DG_FlatFile, DG_FlatFile_add3, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , TRANSFORM(RECORDOF(DG_FlatFile_add3)-filepos, SELF := RIGHT), KEYED(Files.DG_IndexFile));
output(fkj);
