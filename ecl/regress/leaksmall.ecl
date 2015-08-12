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

import dt;

export ebcdic_dstring(ebcdic string del) := TYPE
export integer physicallength(ebcdic string s) := StringLib.EbcdicStringUnboundedUnsafeFind(s,del)+length(del)-1;
export string load(ebcdic string s) := s[1..StringLib.EbcdicStringUnboundedUnsafeFind(s,del)-1];
export ebcdic string store(string s) := (ebcdic string)s+del;
END;


layout_L90_source := record
       ebcdic_dstring((ebcdic string2) x'bf02') name;
       ebcdic_dstring(',')  address1;
   END;

d := dataset('l90::source_block', layout_L90_source ,flat);
output(choosen(d,2), {(string10)name})

