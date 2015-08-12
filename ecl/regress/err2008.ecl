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

MyType := TYPE
    EXPORT INTEGER Load(INTEGER x) := x+1;
    EXPORT INTEGER Store(INTEGER x) := x-1;
END;

MyRec := RECORD
   MyType(3) abc;
END;

NeedC(INTEGER len) := TYPE
    EXPORT String Load(String S) := 'C'+S[1..len];
    EXPORT String Store(String s) := S[2..len+1];
    EXPORT Integer PhysicalLength(String s) := len;
    END;

Rec := RECORD
   NeedC(3)   good;
   NeedC(3,5) abc;
   NeedC xyz;
END;
