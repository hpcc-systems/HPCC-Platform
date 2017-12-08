/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//noversion multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4522, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

rawfile2 := DATASET([{Files.DG_fnames[1]},{'FRED'}],{STRING10 DG_FirstName});
rawfile1 := Files.DG_FlatFile;
index1   := Files.DG_IndexFile;

rawfile2 doJoin(rawfile2 l) := TRANSFORM
            SELF := l;
            END;

rawfile2 doJoin1(rawfile2 l, rawfile1 r) := TRANSFORM
            SELF := l;
            END;

rawfile2 doJoin2(rawfile2 l) := TRANSFORM
            SELF := l;
            END;

hkjoin1 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin(LEFT), LEFT OUTER);
fkjoin1 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin1(LEFT, RIGHT), LEFT OUTER, KEYED(index1));
hkjoin2 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin(LEFT), LEFT ONLY);
fkjoin2 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin1(LEFT, RIGHT), LEFT ONLY, KEYED(index1));
hkjoin3 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin(LEFT));
fkjoin3 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = Files.DG_fnames[1], doJoin1(LEFT, RIGHT), KEYED(index1));

  output(SORT(hkjoin1, DG_FirstName));
  output(SORT(fkjoin1, DG_FirstName));
  output(SORT(hkjoin2, DG_FirstName));
  output(SORT(fkjoin2, DG_FirstName));
  output(SORT(hkjoin3, DG_FirstName));
  output(SORT(fkjoin3, DG_FirstName));

/*
'First two results are LEFT OUTER and should have n+1 records';
'Next two results are LEFT ONLY and should have only 1 record';
'Next two results are INNER and should have n records';*/
