/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

// Checking you can have more than one ONCE section

d1 := DICTIONARY([{1=>2}], { unsigned a=>unsigned b}) : ONCE;
d2 := DICTIONARY([{3=>4}], { unsigned a=>unsigned b}) : ONCE;

unsigned v := 0 : STORED('v');

d1[v].b + d2[v].b;
