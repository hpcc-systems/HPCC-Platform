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

ds := dataset('ds',{string10 first_name; string10 last_name; }, flat);

add(virtual dataset({string10 name;}) d1, virtual dataset({string10 name;}) d2, string10 s1, string10 s2) :=
  d1(name=s1)+d2(name=s2);

output(add(ds{name:=first_name}, ds{name:=last_name}, 'tom', 'john'));
