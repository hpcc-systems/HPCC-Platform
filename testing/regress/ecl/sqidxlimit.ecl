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

//Thor doesn't currently support catch correctly, or in a child query, so disable.
//nothor
//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

filtered(STRING name, unsigned klimit, unsigned slimit) := FUNCTION
    f := sq.SimplePersonBookIndex(surname = name);
    lim := LIMIT(f, klimit, KEYED);
    x := LIMIT(lim, slimit);
    RETURN CATCH(x, SKIP);
END;


ds1 := DATASET([
            {'Halliday', 4, 3},
            {'Halliday', 3, 2}
            ], { string name, unsigned klimit, unsigned slimit });

p1 := TABLE(ds1, { cnt := COUNT(NOFOLD(filtered(name, klimit, slimit))) });



sequential(
output(filtered('Halliday', 4, 3));
output(filtered('Halliday', 4, 2));
output(p1);
);
