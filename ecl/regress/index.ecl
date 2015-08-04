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

person := dataset('person', { unsigned8 person_id, unsigned per_xval, string40 per_first_name, string40 per_last_name, data9 per_cid, unsigned8 xpos }, thor);

myNames := [ person.per_last_name+'x', person.per_last_name+'a', person.per_last_name+'z', person.per_last_name+'v'];
myValues := [person.per_xval+10,person.per_xval, person.per_xval+20, person.per_xval+5 ];

output(person,{myNames[RANKED(1,myNames)],myValues[RANKED(1,myValues)]});


