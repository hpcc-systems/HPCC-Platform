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

idRecord := { unsigned id; };

r := RECORD
unsigned id;
DATASET(idRecord) ids;
END;

r mkR(unsigned i) := TRANSFORM
   temp := '<row><id>' + (string)i + '</id><ids><Row><id>100</id></Row><Row><id>' + (string)(i + 1000) + '</id></Row></ids></row>';
   SELF := FROMXML(r, temp);
END;
 
d := DATASET(20000000, mkR(COUNTER));
output(COUNT(d));
