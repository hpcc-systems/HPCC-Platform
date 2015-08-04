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

100U=100;
100u=100;
0b100U=4;
0x100U=256;
100BU=100b;
100XU=256;
100xu=100x;

0xFFFFFFFFFFFFFFFFU % 6U;
nofold(0xFFFFFFFFFFFFFFFFU) % 6U;
nofold(0x7FFFFFFFFFFFFFFFU) % 6U;
