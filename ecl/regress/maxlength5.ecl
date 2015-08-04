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

r1 := record
    string f1{maxlength(99)}
end;

r2 := record,maxlength(1000)
    string f1{maxlength(99)}
end;

r3 := record,maxlength(50)
    string f1{maxlength(99)}
end;

output(dataset('d1', r1, thor));
output(dataset('d2', r2, thor));
output(dataset('d3', r3, thor));
