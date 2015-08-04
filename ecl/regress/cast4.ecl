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

STRING30 rid := (STRING30)123456789;
output(rid[1..10]); // =''
output(rid[20..30]);

real r := 1.23456;
output((string1)r);
output((string5)r);
output((string10)r);

integer i := -12345;

output((string1)i);
output((string5)i);
output((string6)i);
output((string10)i);

unsigned u := 12345;

output((string1)u);
output((string5)u);
output((string6)u);
output((string10)u);
