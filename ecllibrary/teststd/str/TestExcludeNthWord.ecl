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

EXPORT TestExcludeNthWord := MODULE

  EXPORT TestConst := MODULE
    //Check action on a string with no entries.
    EXPORT Test01 := ASSERT(Str.ExcludeNthWord('',0)+'!' = '!', CONST);
    EXPORT Test02 := ASSERT(Str.ExcludeNthWord('',1)+'!' = '!', CONST);
    EXPORT Test03 := ASSERT(Str.ExcludeNthWord('',-1)+'!' = '!', CONST);
    EXPORT Test04 := ASSERT(Str.ExcludeNthWord('             ',0)+'!' = '!', CONST);
    EXPORT Test05 := ASSERT(Str.ExcludeNthWord('             ',1)+'!' = '!', CONST);
    EXPORT Test06 := ASSERT(Str.ExcludeNthWord('             ',-1)+'!' = '!', CONST);
    //Check action on a string containing a single word - with various whitespace
    EXPORT Test07 := ASSERT(Str.ExcludeNthWord('x',0)+'!' = 'x!');
    EXPORT Test08 := ASSERT(Str.ExcludeNthWord('x',1)+'!' = '!');
    EXPORT Test09 := ASSERT(Str.ExcludeNthWord('x',2)+'!' = 'x!');
    EXPORT Test10 := ASSERT(Str.ExcludeNthWord('x',3)+'!' = 'x!');
    EXPORT Test11 := ASSERT(Str.ExcludeNthWord(' x',1)+'!' = '!');
    EXPORT Test12 := ASSERT(Str.ExcludeNthWord('x ',1)+'!' = '!');
    EXPORT Test13 := ASSERT(Str.ExcludeNthWord(' x',2)+'!' = ' x!');
    EXPORT Test14 := ASSERT(Str.ExcludeNthWord(' x ',1)+'!' = '!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Str.ExcludeNthWord(' abc def ', 1)+'!' = 'def !');
    EXPORT Test16 := ASSERT(Str.ExcludeNthWord(' abc def ', 2)+'!' = ' abc !');
    EXPORT Test17 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',0)+'!' = '  a b c   def    !');
    EXPORT Test18 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',1)+'!' = 'b c   def    !');
    EXPORT Test19 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',2)+'!' = '  a c   def    !');
    EXPORT Test20 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',3)+'!' = '  a b def    !');
    EXPORT Test21 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',4)+'!' = '  a b c   !');
    EXPORT Test22 := ASSERT(Str.ExcludeNthWord('  a b c   def    ',5)+'!' = '  a b c   def    !');
    EXPORT Test23 := ASSERT(Str.ExcludeNthWord(' ,,,, ',1)+'!' = '!');
    //Test other space characters (< 0x20)
    EXPORT Test24 := ASSERT(Str.ExcludeNthWord('  a b\nc \t  def    ',2)+'!' = '  a c \t  def    !');
    EXPORT Test25 := ASSERT(Str.ExcludeNthWord('  a b\nc \t  def    ',3)+'!' = '  a b\ndef    !');
  END;

END;
