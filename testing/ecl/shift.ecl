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

#option ('globalFold', true);

integer one := 1                            : stored('one');
integer n1 := 0x12345                       : stored('n1');
unsigned8 n2 := 0xFEDCBA9876543210          : stored('n2');
integer8 n3 := 0xFEDCBA9876543210           : stored('n3');


output('**'+(string)0x400+'**');
output(1 << 10);
output(one << 10);
output('**'+(string)0x1234+'**');
output((unsigned8)0x12345 >> 4);
output(n1 >> 4);
output('**'+(string)(unsigned8)0x0FEDCBA987654321+'**');
output((unsigned8)0xFEDCBA9876543210 >> 4);
output(n2 >> 4);
output('**'+(string)0xFFEDCBA987654321+'**');
output((integer8)0xFEDCBA9876543210 >> 4);
output(n3 >> 4);
