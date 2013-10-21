/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

baseType := unsigned2;
myEnum1 := ENUM(baseType, one, two, three);
output(sizeof(myEnum1));

myEnum2 := ENUM(one, two, three);
output(sizeof(myEnum2));

myEnum3 := ENUM(integer3, one, two, three);
output(sizeof(myEnum3));

myEnum4 := ENUM(integer8, one, two, three);
output(sizeof(myEnum4));
