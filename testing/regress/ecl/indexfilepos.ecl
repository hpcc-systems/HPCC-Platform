/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

//Test the different variants of fileposition fields
// - fixed and variable with records
// - 3 different index types
// - 2 different ways of defining the index (from dataset or from record)

//version compressionType='legacy',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='legacy',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='legacy',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=false
//version compressionType='legacy',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='legacy',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='legacy',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=false

//version compressionType='legacy',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='legacy',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='legacy',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=true
//version compressionType='legacy',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='legacy',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='legacy',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=true

//version compressionType='inplace',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='inplace',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='inplace',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=false
//version compressionType='inplace',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='inplace',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='inplace',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=false

//version compressionType='inplace',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='inplace',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='inplace',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=true
//version compressionType='inplace',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='inplace',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='inplace',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=true

//version compressionType='hybrid',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='hybrid',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='hybrid',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=false
//version compressionType='hybrid',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=false
//version compressionType='hybrid',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=false
//version compressionType='hybrid',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=false

//version compressionType='hybrid',variableWidth=false,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='hybrid',variableWidth=false,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='hybrid',variableWidth=false,zeroFilePos=true,hasFilePos=true,fromDataset=true
//version compressionType='hybrid',variableWidth=true,zeroFilePos=false,hasFilePos=false,fromDataset=true
//version compressionType='hybrid',variableWidth=true,zeroFilePos=false,hasFilePos=true,fromDataset=true
//version compressionType='hybrid',variableWidth=true,zeroFilePos=true,hasFilePos=true,fromDataset=true

import ^ as root;
import $.setup;
import Std.File AS FileServices;

compressionType := #IFDEFINED(root.compressionType, 'hybrid');
variableWidth := #IFDEFINED(root.variableWidth, true);
zeroFilePos := #IFDEFINED(root.zeroFilePos, true);
hasFilePos := #IFDEFINED(root.hasFilePos, true);
fromDataset := #IFDEFINED(root.fromDataset, true);

idxRecord := RECORD
    unsigned uid
    =>
    unsigned uid2;
#if (variableWidth)
    string pad := '<pad>';
#end
#if (not zeroFilePos)
    unsigned8 uid3;
#end
END;

numParents := 50;

ds := DATASET(numParents,
            TRANSFORM(idxRecord,
                SELF.uid := COUNTER;
                SELF.uid2 := HASH32(COUNTER);
#if (variableWidth)
                SELF.pad := 'Arfle Barfle Gloop!!'[1..COUNTER%20];
#end
#if (not zeroFilePos)
                SELF.uid3 := COUNTER;
#end
            ), DISTRIBUTED);


prefix := setup.Files(false, false).indexPrefix + WORKUNIT;
indexName := prefix+'_fpos_index';

#if (fromDataset)
i := INDEX(ds, { uid }, {ds}, indexName, compressed(compressionType),FILEPOSITION(hasFilePos));
#else
i := INDEX(idxRecord, indexName, compressed(compressionType),FILEPOSITION(hasFilePos));
#end

// NOTE: use recordof(i) in the following to ensure that any implicit file position is included
lhs := PROJECT(SORT(ds, uid2), TRANSFORM(recordof(i), SELF := LEFT));

matchRecord := RECORD
 recordof(i) l;
 recordof(i) r;
END;

j := JOIN(lhs, i, LEFT.uid=RIGHT.uid, TRANSFORM(matchRecord, SELF.l := LEFT; SELF.r := RIGHT), KEEP(1));

SEQUENTIAL(
 BUILD(i, ds, OVERWRITE);
 OUTPUT(j(l != r));

 // Clean-up
 FileServices.DeleteLogicalFile(indexName)
);
