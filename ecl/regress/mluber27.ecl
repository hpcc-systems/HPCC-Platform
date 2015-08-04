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

loadxml('<__x_xr/>');

//Constant fold at generate time
#declare(constMyYear)
#SET(constMyYear,(unsigned4)(stringlib.getDateYYYYMMDD()[1..4]));
#STORED('myYear2',%constMyYear%);
unsigned4 myYear2 := 0 : stored('myYear2');

//Implement at runtime.
#STORED('myYear',(unsigned4)(stringlib.getDateYYYYMMDD()[1..4]));
unsigned4 myYear := 0 : stored('myYear');


output(myYear);
output(myYear2);
