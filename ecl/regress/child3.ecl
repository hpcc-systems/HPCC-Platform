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


ds := dataset([{1, ['a','b','c']}], { unsigned id; dataset({string1 str}) x });

ds t(ds l) := TRANSFORM
    d := dedup(l.x, all)(str <> 'z');
    e := d(str <> (string)count(d));
    self.x := e;
    self := l;
END;

p := project(nofold(ds), t(LEFT));

output(p);
