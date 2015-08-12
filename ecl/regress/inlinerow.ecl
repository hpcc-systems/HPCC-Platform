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

rec1 := record
                string1 c;
                string2 d;
end;

rec2 := record
                string1 a;
                rec1 b;
end;


r0 := row({'2', '34'}, rec1);

r1 := row({'1', {'2', '34'}}, rec2);

r2 := row({'1', r0}, rec2);

output(dataset(r2));
