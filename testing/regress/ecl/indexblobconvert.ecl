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

#onwarning (4523, ignore);

//version compression='hybrid'
//version compression='hybrid:blob(lzw)'
//version compression='hybrid:blob(zstd)'
//version compression='hybrid:blob(zstd3)'
//version compression='hybrid:blob(zstd6)'
//version compression='hybrid:blob(zstd9)'
//version compression='inplace:zstds6'

import ^ as root;
import $.setup;
import Std.File AS FileServices;

noSeek := #IFDEFINED(root.noSeek, true);
compressionType := #IFDEFINED(root.compression, 'hybrid');

#option ('noSeekBuildIndex', noSeek);

prefix := setup.Files(false, false).IndexPrefix + WORKUNIT + '::';

grandchildRec := RECORD
 string f2;
 string f3;
END;

childRec := RECORD
 string f1;
 DATASET(grandchildrec) gkids;
END;

parentRec := RECORD
 unsigned uid;
 DATASET(childRec) kids1;
END;

numParents := 10000;
numKids := 10;
numGKids := 5;

d3(unsigned c) := DATASET(numGKids, TRANSFORM(grandchildrec, SELF.f2 := (string)COUNTER+c;
                                                             SELF.f3 := (string)HASH(COUNTER)
                                              )
                         );
d2(unsigned c) := DATASET(numKids, TRANSFORM(childRec, SELF.f1 := (string)c+COUNTER,
                                                       SELF.gkids := d3(COUNTER+c)));
ds := DATASET(numParents, TRANSFORM(parentRec, SELF.uid := COUNTER;
                                               SELF.kids1 := d2(COUNTER);
                                   ), DISTRIBUTED);



idxRecord := RECORD
 unsigned uid;
 DATASET(childRec) payload{BLOB};
END;

indexName1 := prefix+'testindex';
indexName2 := indexName1 + '_' + compressionType;

p := PROJECT(ds, TRANSFORM(idxRecord, SELF.payload := LEFT.kids1; SELF := LEFT));

//Duplicate the uid into the "fileposition" field so the value can be verified.
i1 := INDEX(p, {uid}, {p, unsigned8 uid2 := uid }, indexName1, compressed('legacy'));
i2 := INDEX(i1, indexName2);

compareFiles(ds1, ds2) := FUNCTIONMACRO
    c := COMBINE(ds1, ds2, transform({ boolean same, RECORDOF(LEFT) l, RECORDOF(RIGHT) r,  }, SELF.same := LEFT = RIGHT; SELF.l := LEFT; SELF.r := RIGHT ), LOCAL);
    RETURN output(choosen(c(not same), 10));
ENDMACRO;


SEQUENTIAL(
    BUILD(i1, OVERWRITE);

    FileServices.Copy(
            sourceLogicalName := indexName1,
            destinationLogicalName := indexName2,
            destinationGroup := '',
            replicate := false,
            noSplit := true,
            allowoverwrite := true,
            keyCompression := compressionType);

    compareFiles(i1, i2);

    FileServices.DeleteLogicalFile(indexName1);
    FileServices.DeleteLogicalFile(indexName2);
);
