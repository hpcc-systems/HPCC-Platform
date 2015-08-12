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

IMPORT Std.Str;

EXPORT TestExcludeFirstWord := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.ExcludeFirstWord('')+'!' = '!', CONST);
    EXPORT Test04 := ASSERT(Str.ExcludeFirstWord('             ')+'!' = '!', CONST);
    EXPORT Test07 := ASSERT(Str.ExcludeFirstWord('x')+'!' = '!');
    EXPORT Test11 := ASSERT(Str.ExcludeFirstWord(' x')+'!' = '!');
    EXPORT Test12 := ASSERT(Str.ExcludeFirstWord('x ')+'!' = '!');
    EXPORT Test15 := ASSERT(Str.ExcludeFirstWord(' abc def ')+'!' = 'def !');
    EXPORT Test17 := ASSERT(Str.ExcludeFirstWord(' a b c   def ')+'!' = 'b c   def !');
    EXPORT Test18 := ASSERT(Str.ExcludeFirstWord(' ,,,, ')+'!' = '!');
    EXPORT Test19 := ASSERT(Str.ExcludeFirstWord(' ,,,, ,,, ')+'!' = ',,, !');
  END;

END;
