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

//#WHEN(LEGACY)

i := index({ unsigned8 id1, unsigned8 id2 }, {}, 'i');
k := i(wild(id1) and keyed(id2 = 100))[1].id1;

f() := FUNCTION
    output(k);
    name := 'per';

    p := 10 : persist(name);
    RETURN p;
END;

output(f());
