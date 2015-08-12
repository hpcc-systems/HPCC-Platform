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

export xRecord := RECORD
data9 zid;
    END;

export xDataset := DATASET('x',xRecord,FLAT);


output(xDataset(zid in [
x'01a48d8414d848e900',
x'01a48d879c3760cf01',
x'01a48ec793a76f9400',
x'01a48ecd6e65d8e803',
x'01a48ed1bb70d84c01',
x'01a48ed8f40385ba01',
x'01a490101d3de9ac02',
x'01a4901558d7c91900',
x'01a491479336b4d500',
x'01a4914d5b326b9202',
x'01a49155d1caf41500',
x'01a49156089fce5a02',
x'01a4916c37db816c02']));


