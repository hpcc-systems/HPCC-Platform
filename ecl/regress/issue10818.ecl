/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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


idRec := { unsigned x; unsigned y; };

inRecord :=
            RECORD
unsigned        id;
dataset(idRec)  ids;
            END;

outRecord :=
            RECORD
unsigned        id;
unsigned        cnt;
            END;


r(dataset(outRecord) prev, unsigned iter) := FUNCTION
inTable := dataset([{iter,[{1,1},{1,2}]},{iter+1,[{3,1},{1,4}]}], inRecord);

outRecord t(inRecord l) := TRANSFORM
    SELF.id := l.id;
//    SELF.cnt := COUNT(NOFOLD(JOIN(l.ids, l.ids, LEFT.y = RIGHT.x, MANY LOOKUP, lOCAL)));
    SELF.cnt := COUNT(NOFOLD(JOIN(l.ids, l.ids, LEFT.x = RIGHT.y, MANY LOOKUP)));
END;

RETURN PROJECT(inTable, t(LEFT));

END;


nullDs := DATASET([], outRecord);
l := LOOP(nullDs, 4, r(ROWS(LEFT), COUNTER));

output(l);
