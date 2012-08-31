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

