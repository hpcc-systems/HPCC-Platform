/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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