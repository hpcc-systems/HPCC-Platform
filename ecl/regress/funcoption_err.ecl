//#when(LEGACY)
/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

// Catch error case of a #stored preceding a function

a := MODULE
  
   EXPORT b(UNSIGNED x) := MODULE
      EXPORT cnt := x;
   END;

   #stored ('oneTwoThree', 123);
   export z := b;
END;

a.z(3).cnt;
