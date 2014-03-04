/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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


value := 12345.678901234;

output(value);
output((string)value);
output((string4)value);
output((string4)nofold(value));
output((string5)value);
output((string5)nofold(value));
output((string6)value);
output((string6)nofold(value));
output((string7)value);
output((string7)nofold(value));
output((string8)value);
output((string8)nofold(value));
output((string20)value);
output((string20)nofold(value));

value2 := 0.01456;

output((string1)value2);
output((string2)value2);
output((string3)value2);
output((string4)value2);
output((string5)value2);
output((string6)value2);
output((string7)value2);

output((string5)1.9994);
output((string5)1.9995);
output((string5)9.9995);
output((string5)9.99951);

output((string5)-1.994);
output((string5)-1.995);
output((string5)-9.995);
output((string5)-9.995001);

output((string5)-199.4);
output((string5)-199.6);
output((string5)-1999.4);
output((string5)-1999.6);
output((string5)-9999.4);
output((string5)-9999.6);
