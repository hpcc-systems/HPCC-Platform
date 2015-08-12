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

(string)(decimal1_1) 5.6;
(string)(decimal2_2) 5.06;
output((string)(DECIMAL4_1)0);              // = 0.0
output((string)(DECIMAL4_1)0.0);            // = 0.0
output((string)(DECIMAL5_2)999.999);        // = 0.0  loss of precision


(decimal1_1) 5.6;
(decimal2_2) 5.06;
output((DECIMAL4_1)0);              // = 0.0
output((DECIMAL4_1)0.0);            // = 0.0
output((DECIMAL5_2)999.999);        // = 0.0  loss of precision
