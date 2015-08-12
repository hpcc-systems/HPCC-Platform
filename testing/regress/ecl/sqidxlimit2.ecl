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

#option ('globalAutoHoist', false);

//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

ds2 := DATASET([
            {'Halliday', 4},
            {'Halliday', 1},
            {'Halliday', 0}, // should be empty
            {'Zingo', 2}
            ], { string name, unsigned climit });

filtered2(STRING name, unsigned climit) := FUNCTION
    f := sq.SimplePersonBookIndex(surname = name);
    RETURN CHOOSEN(f, climit);
END;

p2 := TABLE(ds2, { cnt := COUNT(NOFOLD(filtered2(name, climit))) });

ds3 := DATASET([
            {'Harper Lee', 4},
            {'Harper Lee', 1},
            {'Harper Lee', 0}, // should be empty
            {'Various', 1000},
            {'Various', 1},
            {'Zingo', 2}
            ], { string name, unsigned climit });

filtered3(STRING searchname, unsigned climit) := FUNCTION
    f := sq.SimplePersonBookIndex.books(author = searchname);
    RETURN CHOOSEN(f, climit);
END;

p3 := TABLE(ds3, { cnt := COUNT(NOFOLD(filtered3(name, climit))) });


sequential(
output(p2);
output(p3);
);
