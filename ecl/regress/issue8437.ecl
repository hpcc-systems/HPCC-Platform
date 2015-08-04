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


r1 := { unsigned id, string name; string name2};

r3 := { dataset(r1) children, dataset(r1) children2 };
r2 := { unsigned id, r3 r };

s := dataset([{0,[{1,'Gavin','Hawthorn'},{2,'Jason','Jones'}],[]}], r2);

r2 t(r2 l) := TRANSFORM
    SELF.r.children := DISTRIBUTE(l.r.children, HASH(id));
    SELF := l;
END;

p := PROJECT(NOFOLD(s), t(LEFT));
output(p);
