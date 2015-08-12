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

#option ('globalFold', false);
#link ('hthor');
import lib_stringlib;

loadxml('<AA><BB>1</BB><CC>2</CC></AA>');

#DECLARE (x,y)

#SET (x, RANDOM()*0)
#SET (y, stringlib.GetDateYYYYMMDD()[1..4])

%'x'%; '\n';
%'y'%; '\n';
(integer8)RANDOM(); '\n';
(string10)stringlib.GetDateYYYYMMDD(); '\n';
