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

//Error: The reference to words.word in the OUTPUT should complain that "words" is not in scope
//Unfortunately it gets optimized away by EXISTS(x) always being true.  Check a warning is output.

#option ('pickBestEngine', false);
#onwarning (1051, warning);

IMPORT SerialTest;

interestingWords := DICTIONARY([{'elves'},{'cheddar'}], SerialTest.wordRec);

output(SerialTest.bookIndex(WILD(title), EXISTS(words.word IN interestingWords)));

