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

d := dataset('~people', { string15 name, unsigned8 dob }, flat);

// The parameter can be any compile-time constant?  It "could" be supported, but isn't currently
//If you really need something like this it will need to use a macro.
//It is an example of a function needing to have a fixed output record format.
i(boolean hasFileposition) := index(d, { name, dob }, 'peopleIndex', FILEPOSITION(hasFileposition)); // dob goes into the keyed portion, not filepos
output(i(false)(name = 'Gavin' and dob != 0));
