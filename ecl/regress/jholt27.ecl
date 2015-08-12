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

rec1 := RECORD
STRING str;
END;
rtypes := ENUM(w1, w2);
rec2 := RECORD
STRING str;
rtypes x;
SET OF INTEGER seq;
END;

rec2 cvt(rec1 l, INTEGER t) := TRANSFORM
SELF.x := t;
SELF.str := l.str;
SELF.seq := [0];
END;
rec2 nbr(rec2 l, INTEGER c) := TRANSFORM
SELF.seq := [c];
SELF := l;
END;

d1 := nofold(DATASET([{'123'}, {'124'}], rec1));
d2 := nofold(DATASET([{'234'}, {'235'}], rec1));
s1 := PROJECT(d1, cvt(LEFT, rtypes.w1));
s2 := PROJECT(d2, cvt(LEFT, rtypes.w2));

s := PROJECT(s1+s2, nbr(LEFT, COUNTER));

OUTPUT(s);

