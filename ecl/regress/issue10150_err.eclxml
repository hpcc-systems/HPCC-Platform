<Archive useLocalSystemLibraries="1">
 <Query attributePath="regress.module7"/>
 <Module key="regress" name="regress">
  <Attribute key="module7" name="module7" sourcePath="module7.ecl">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

import Std.Str;
import Std.Str.^.^.Std.Uni;  // Invalid to access parent of Std?

output(Str.toUpperCase(&apos;x&apos;));
output(Uni.toUpperCase(U&apos;x&apos;));&#10;
  </Attribute>
 </Module>
</Archive>
