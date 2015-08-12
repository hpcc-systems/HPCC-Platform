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

RETURN MODULE

import lib_StringLib, lib_unicodeLib;

export vstring(integer i) := TYPE
    export integer physicallength(string s) := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s;
    END;

// attribute to define variable length field where the 1st byte is the length

export pstring := type
    export integer physicallength(string x) := transfer(x[1], unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
    export integer maxLength := 256;
end;

export ebcdic_pstring := type
    export integer physicallength(ebcdic string x) := transfer(x[1], unsigned1)+1;
    export string load(ebcdic string x) := x[2..transfer(x[1], unsigned1)+1];
    export ebcdic string store(string x) := transfer(length(x), string1)+x;
    export integer maxLength := 256;
end;

export ebcdic_dstring(ebcdic string del) := TYPE
    export integer physicallength(ebcdic string s) := StringLib.EbcdicStringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(ebcdic string s) := s[1..StringLib.EbcdicStringUnboundedUnsafeFind(s,del)-1];
    export ebcdic string store(string s) := (ebcdic string)s+del;
END;


// data type to handle delimited data

export dstring(string del) := TYPE
    export integer physicallength(string s) := StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(string s) := s[1..StringLib.StringUnboundedUnsafeFind(s,del)-1];
    export string store(string s) := s+del; // Untested (vlength output generally broken)
END;

export ustring(unicode del) := TYPE
    export integer physicallength(unicode s) := (UnicodeLib.UnicodeFind(s,del,1)+length(del)-1)*2;
    export unicode load(unicode s) := s[1..UnicodeLib.UnicodeFind(s,del,1)-1];
    export unicode store(unicode s) := s+del; // Untested (vlength output generally broken)
END;

END;