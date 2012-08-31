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

loadxml('<section><ditem><val>person_id</val></ditem></section>');

#DECLARE (attrib_name)
#DECLARE (flag)

#SET (attrib_name, 'perssson.')

#FOR (ditem)
     #APPEND (attrib_name, %'val'%)     // Now ... attrib_name = 'dms_person.dms_per_id'
     #SET(flag, #ISVALID(%attrib_name%))
#END

export out1 := %flag%;
out1