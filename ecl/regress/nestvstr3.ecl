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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
vstring(integer i) := TYPE
    export integer physicallength(string s) := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s;
    END;


vdata(integer i) := TYPE
    export integer physicallength(data s) := i;
    export data load(data s) := s[1..i];
    export data store(data s) := s;
    END;

rawLayout := record
    string20 dl;
    string8 date;
    unsigned2 imgLength;
    vdata(SELF.imgLength) jpg;
end;

d1 := dataset([{'id1', '20030911', 5, x'1234567890'}, {'id2', '20030910', 3, x'123456'}], rawLayout);
//output(d1,,'imgfile', overwrite);

d := dataset('imgfile', { rawLayout x, unsigned8 _fpos{virtual(fileposition)} }, FLAT);
output(d);
