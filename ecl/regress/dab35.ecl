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

vstring(integer i) := TYPE
    export integer physicallength(string s) := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s;
    END;

r3 := record
  unsigned integer1 len;
  vstring(self.len) p;
  end;

d := dataset('temp.dab',r3,flat);

r2 := record
  string20 p;
  end;

r2 trans(r3 le) := transform
  self := le;
  end;

pr := project(d,trans(left));

output(pr);
