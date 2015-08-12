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

IMPORT * from lib_stringlib;

UNSIGNED4 FindUnbounded(STRING src, STRING terminator) :=
  StringLib.StringUnboundedUnsafeFind(src, terminator);

STRING stored_blank := '' : STORED('stored_blank');
STRING5 stored_Gavin := 'Gavin' : STORED('stored_Gavin');

EXPORT TestFindUnbounded := MODULE

  EXPORT TestRuntime := MODULE
    EXPORT Test1 := ASSERT(FindUnbounded(TRANSFER(stored_Gavin + ' ' + stored_Gavin, string10), 'Gavin') = 1);
    EXPORT Test2 := ASSERT(FindUnbounded(TRANSFER(stored_Gavin + ' ' + stored_Gavin, string10), ' Gavin') = 6);
  END;

END;

output(TestFindUnbounded);
