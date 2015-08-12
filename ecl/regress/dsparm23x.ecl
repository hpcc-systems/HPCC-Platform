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

// A join example

r1 := record
 String10 first_name1;
 string20 last_name1;
end;

r2 := record
 String10 first_name2;
 string20 last_name2;
end;

ds := dataset('ds', r1, FLAT);
dsx := dataset('dsx', r2, FLAT);

rec := record
  string10 id;
end;

rec tranx(r1 l, r2 r) := transform
  self.id := l.first_name1;
end;


myjoin := Join(ds, dsx, left.first_name1 = right.first_name2, tranx(left,right));

count(myjoin);

