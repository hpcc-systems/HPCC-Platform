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

r1 := RECORD
    unsigned1   f1{default(0)};
    unsigned1   f2{default(255)};
    unsigned1   badf3{default(256)};
    bitfield3   f4{default(7)};
    bitfield3   badf5{default(8)};
END;

output(dataset(row(transform(r1, self := []))));
