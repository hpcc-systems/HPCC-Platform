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

// ---- Start ECL code ----

tempRec := record
        unicode overflow   {maxlength(5000000)};
end;
tempVRec := record
        varunicode overflow   {maxlength(5000000)};
end;

ds1 := dataset([{'This is some string'},
                {''}], tempRec);
ds2 := dataset([{'This is some string'},
                {''}], tempVRec);


output(ds1(overflow!=u''));     // fine
output(ds1(overflow!=''));     // fine
output(ds2(overflow!=u''));    // fine

output(ds2(overflow!=''));  // EclServer Terminated Unexpectedly

// ---- End ECL code ----
