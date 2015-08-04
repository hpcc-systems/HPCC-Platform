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

// test: similar to test 9, but f return integer instead of dataset
ds := dataset('ds', {String10 first_name; string20 last_name; }, FLAT);

dataset f(virtual dataset({String10 name;}) d) := d(name = 'fred');

integer g(virtual dataset d) := count(f(d));

g(ds{name:=first_name});
