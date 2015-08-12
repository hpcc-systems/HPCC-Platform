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

import Text;
ds := DATASET([{'hello my'}, {'name is ed'}], {STRING line});

PATTERN inPattern := (Text.alpha|'-')+;

outl :=
RECORD
    ds.line;
    STRING wrd;
END;

outl t(ds f) :=
TRANSFORM
    SELF.wrd := MATCHTEXT(inPattern);
    SELF.line := f.line;
END;
outp := PARSE(ds, line, inPattern, t(LEFT), MAX);
output(outp);
