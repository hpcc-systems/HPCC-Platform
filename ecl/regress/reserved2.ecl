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

//Perverse syntax to demonstrate the lack of hard reserved words....
import dt;

integer PARSE := 1234;
unsigned4 FULL := 100;

output(PARSE * full);

r := record,maxlength(12345)
    unsigned4 group;
    boolean flat;
    boolean all := false;
    boolean dt;
    boolean dt;
    string maxlength := '123';
    end;


ds := dataset('ds', r, thor);
string full := 'z';


ds2 := dedup(ds, ds.group, ds.all, all);

output(ds2, { ds2.group });


t := table(ds2, {unsigned8 group := ds2.group; ds2.flat, ds2.all, ds2.dt; (unsigned8)flat });

output(t,,'out.xml',xml);

output(ds,,'out2.xml',xml);