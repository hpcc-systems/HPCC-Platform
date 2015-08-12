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

vrec := RECORD
INTEGER4 grp;
INTEGER4 id := 0;
INTEGER1 skp := 0;
INTEGER1 hasopt := 0;
IFBLOCK(SELF.hasopt>0)
INTEGER4 optqnt := 0;
END;
INTEGER4 qnt := SELF.id;
INTEGER4 cnt := 0;
END;

vset := DATASET([{1, 1}, {1, 2}, {1, 3, 0, 1, 99}, {1, 4, 0, 1, 1}, {2, 1}, {2, 2}, {2, 3}, {2, 4, 1}, {3, 1, 1}, {4, 1}, {4, 2, 1}, {4, 3}, {4, 4}], vrec);
output(vset);

