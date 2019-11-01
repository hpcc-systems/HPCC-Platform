/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
//version expandHashJoin=false
//version expandHashJoin=true

//NB: hash join not needed/implemented in hthor or roxie
//noroxie
//nohthor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
expandHashJoin := #IFDEFINED(root.expandHashJoin, false);

#option('expandHashJoin', expandHashJoin);

import $.setup;
Files := setup.Files(multiPart, useLocal, false);

recpair := record
    string45 name;      //join type description
    string45 leftrec;
    string45 rightrec;
  END;

j := JOIN(Files.DG_FlatFile, Files.DG_FlatFileEvens, left.DG_firstname = right.DG_firstname AND left.DG_lastname=right.DG_lastname, HASH);

output(SORT(j, RECORD));
