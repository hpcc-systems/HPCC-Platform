/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

testRecord := RECORD
    STRING name1{LENGTHSIZE(3)};
    STRING name2{LENGTHSIZE(7)};
    INTEGER name4{LENGTHSIZE(4)};
    DATASET({unsigned id}) ds{LENGTHSIZE(4)};
END;

testRecord2 := RECORD
    STRING name{LENGTHSIZE};
END;
