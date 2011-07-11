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

ReverseString4 := TYPE
    SHARED STRING4 Rev(String4 S) := s[4]+s[3]+s[2]+s[1];
    EXPORT String4 Load(String4 s) := Rev(s);
    EXPORT String4 Store(String4 s) := Rev(s);
END;


MyType := TYPE
    EXPORT INTEGER Load(INTEGER x) := x+1;
    EXPORT INTEGER Store(INTEGER x) := x-1;
END;

NeedC(INTEGER len) := TYPE
    EXPORT String Load(String S) := 'C'+S[1..len];
    EXPORT String Store(String s) := S[2..len+1];
    EXPORT Integer PhysicalLength(String s) := len;
END;


R := Record
    ReverseString4   F1;
    NeedC(5)     F2;
END;

