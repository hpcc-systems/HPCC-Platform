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

//Make sure both of these don't lose the extra digit.
output((string)(round(9.9D)) + '\n');
output((string)(round(5D, -1)) + '\n');


output((string)(round(nofold(1.1D), 0)) + '\n');
output((string)(round(nofold(1.1D), 1)) + '\n');
output((string)(round(nofold(1.1D), -1)) + '\n');


output((string)(round(1234567, -2)) + '\n');
output((string)(round(nofold(1234567), -2)) + '\n');
