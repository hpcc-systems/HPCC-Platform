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

output((>string<)X'4142434445');    //ABCDE
output((>string3<)X'4142434445');   //ABC
output((>string4<)0x44434241);      //ABCD little endian
output((>string3<)0x44434241);      //ABC little endian
output((>string<)0x44434241);       //ABCDxxxx little endian



output((>integer4<)X'01020304');
output((>integer4<)X'010203040506');


output((>string3<)123456.78D);
output((>string<)123456.78D);
