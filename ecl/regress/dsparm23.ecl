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

// To make this example more useful and more interesting,
// we may need implement "abstract"
// record type for transform so that the transform
// is not as trial as this.

//
// define the func

rec := record
  string10 id;
end;

rec tranx(string10 id) := transform
  self.id := id;
end;

SimpleJoin(virtual dataset({string10 id}) d1, virtual dataset({string10 id}) d2) :=
  join(d1, d2, left.id = right.id, tranx(left.id));

//
// test the func

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


myjoin := SimpleJoin(ds{id:=first_name1}, dsx{id:=first_name2});

count(myjoin);

