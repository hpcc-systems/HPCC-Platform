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

// Test the LENGTHSIZE field attribute

layout_test1 := RECORD
    STRING str1{LENGTHSIZE(1)};
    STRING str2{LENGTHSIZE(2)};
    STRING str3{LENGTHSIZE(4)};
END;

layout_test2 := RECORD
    STRING name{MAXLENGTH(50), LENGTHSIZE(1)};
    STRING address{MAXLENGTH(200), LENGTHSIZE(2)};
END;

ds := DATASET([
    {'hello', 'world', 'test'},
    {'foo', 'bar', 'baz'}
], layout_test1);

OUTPUT(ds);

'Done';
