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
#option ('foldConstantCast', false);

output(intformat(1234, 8, 0));
output(intformat(1234, 8, 1));
output(intformat(1234, 4, 0));
output(intformat(1234, 4, 1));
output(intformat(1234, 3, 0));
output(intformat(1234, 3, 1));

output((integer1)(0x123456789abcdef0+0));
output((integer2)(0x123456789abcdef0+0));
output((integer3)(0x123456789abcdef0+0));
output((integer4)(0x123456789abcdef0+0));
output((integer5)(0x123456789abcdef0+0));
output((integer6)(0x123456789abcdef0+0));
output((integer7)(0x123456789abcdef0+0));
output((integer8)(0x123456789abcdef0+0));

output((unsigned1)(0x123456789abcdef0+0));
output((unsigned2)(0x123456789abcdef0+0));
output((unsigned3)(0x123456789abcdef0+0));
output((unsigned4)(0x123456789abcdef0+0));
output((unsigned5)(0x123456789abcdef0+0));
output((unsigned6)(0x123456789abcdef0+0));
output((unsigned7)(0x123456789abcdef0+0));
output((unsigned8)(0x123456789abcdef0+0));
