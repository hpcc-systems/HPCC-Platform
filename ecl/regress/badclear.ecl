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





set of string mySet := ['one','two','three','four','five','six'];

searchSet := dataset(mySet, { string x{maxlength(50)} });


v1 := true : stored('v1');
v2 := 4 : stored('v2');

ds := dataset([{1},{2},{3},{4},{5},{6},{4},{6}],{ unsigned4 id });

r := record
    unsigned4 id;
    string value{maxlength(50)};
    string value1{maxlength(50)};
end;

r t(recordof(ds) l) := transform
    self.value := searchSet[l.id].x;
    self.value1 := IF(v1, searchSet[v2].x, searchSet[v2-1].x);
    self := l;
end;

p := project(nofold(ds), t(LEFT));

output(p);
