/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

rec := RECORD
    STRING5 name;
    INTEGER2 i;
END;

rec zero(rec l) := TRANSFORM
    SELF.i := 0;
    SELF := l;
END;

rec sumi(rec l, rec r) := TRANSFORM
    SELF.i := l.i + r.i;
    SELF := l;
END;

raw := DATASET([{'adam', 23}, {'eve', 26}, {'adam', 9}, {'eve', 19}, {'adam', 0}
, {'eve', 10}, {'adam', 4}, {'eve', 10}], rec);

inp := GROUP(SORT(raw, name, i), name);

names := PROJECT(DEDUP(inp, name), zero(LEFT));

denorm := DENORMALIZE(names, inp, (LEFT.name = RIGHT.name) AND (LEFT.i < 30), 
sumi(LEFT, RIGHT));

OUTPUT(denorm);

