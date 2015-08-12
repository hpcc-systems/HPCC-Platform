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

import DMS_Test;
//loadxml('<section><ditem><val>dms_per_id</val></ditem></section>');
loadxml('<section><ditem><val>calcAge</val></ditem></section>');

#DECLARE (flag)

#SET(flag, #ISVALID('person.dms_per_id'))
#APPEND(flag, ' ')
#APPEND(flag, #ISVALID('DMS_Test.calcAge(0)'))

export out1 := %'flag'%;
out1;
