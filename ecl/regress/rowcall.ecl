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


r := { unsigned value1, unsigned value2 };
makeRow(unsigned v1, unsigned v2) := ROW(TRANSFORM(r, SELF.value1 := v1; SELF.value2 := v2));

x := SERVICE
       dataset(r) myFunction(dataset(r) x) : entrypoint('doesnotexist');
       integer myFunction2(dataset(r) x) : entrypoint('doesnotexist');
   END;

row1 := makeRow(1,2);

output(x.myFunction(dataset(row1)));

row2 := makeRow(5,6);

output(x.myFunction2(dataset(row2)));
