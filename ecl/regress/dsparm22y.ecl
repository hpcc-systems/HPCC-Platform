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

#option ('globalFold', false);
stTrue := true : stored('stTrue');
stFalse := false : stored('stFalse');

ds := dataset('ds',{string10 first_name1; string10 last_name1; }, flat);
dsx := dataset('dsx',{string10 first_name2; string10 last_name2; }, flat);

f(virtual dataset d1, virtual dataset d2, boolean b) := if(b, d1, d2);

ct1 := count(f(ds, dsx, stTrue));

ct2 := count(f(ds, dsx, stFalse));

ct := ct1+ct2;

ct;
