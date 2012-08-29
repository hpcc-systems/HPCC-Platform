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

// test: more than one level of dataset passing (w/ mapping)

ds := dataset('ds', {String10 first_name; string20 last_name; }, FLAT);

dataset f(virtual dataset({String10 first_name}) d) := d(first_name = 'fred');

dataset g(virtual dataset({String20 last_name}) d) := d(last_name='tom');

dataset h(virtual dataset d) := g(f(d));

ct2 := output(h(ds));

ct2;
