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

shared MyRec := RECORD
STRING3 foo1;
STRING5 foo2;
UNSIGNED1 nullTerm;
END;

test1 := DATASET([{'0123456789'}, {'1234567890'}, {'43210'}], {VARSTRING x});

output(SIZEOF(MyRec));

MyRec xform({VARSTRING x} l) := TRANSFORM
VARSTRING w1 := l.x[1..SIZEOF(MyRec)-1];
self := TRANSFER(w1,MyRec);
END;

t1 := PROJECT(test1, xform(LEFT));

output(t1);

