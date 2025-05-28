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

//Constant patterns - non constant strings.  Cause a crash if generated globally in a stand alone program

REGEXFIND('s.t', NOFOLD('set'));
NOT(REGEXFIND('s.t', 'select'));
REGEXFIND('s.*t', NOFOLD('select'));
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('upset sot resets it should match'));
NOT(REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE')));
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), NOCASE);
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), 0, NOCASE) = 'UPSET SOT RESETS IT ';
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), 1, NOCASE) = 'RESETS IT ';
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), 2, NOCASE) = 'RE';
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), 3, NOCASE) = 'S IT';
REGEXFIND('^((..)?(s ?.t)* )+', NOFOLD('UPSET SOT RESETS IT should match only with NOCASE'), 4, NOCASE) = '';
REGEXFIND('.ook(?=ahead)', NOFOLD('lookahead'));
REGEXFIND('.ook(?=ahead)', NOFOLD('lookahead'), 0) = 'look';
NOT(REGEXFIND('.ook(?=ahead)', NOFOLD('bookmarks')));
NOT(REGEXFIND('.ook(?!ahead)', NOFOLD('lookahead')));
REGEXFIND('.ook(?!ahead)', NOFOLD('bookmarks'));
REGEXFIND('.ook(?!ahead)', NOFOLD('bookmarks'), 0) = 'book';

REGEXFIND(u's.t', NOFOLD(u'set'));
NOT(REGEXFIND(u's.t', NOFOLD(u'select')));
REGEXFIND(u's.*t', NOFOLD(u'select'));
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'upset sot resets it should match'));
NOT(REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE')));
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), NOCASE);
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), 0, NOCASE) = u'UPSET SOT RESETS IT ';
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), 1, NOCASE) = u'RESETS IT ';
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), 2, NOCASE) = u'RE';
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), 3, NOCASE) = u'S IT';
REGEXFIND(u'^((..)?(s ?.t)* )+', NOFOLD(u'UPSET SOT RESETS IT should match only with NOCASE'), 4, NOCASE) = u'';
REGEXFIND(u'.ook(?=ahead)', NOFOLD(u'lookahead'));
REGEXFIND(u'.ook(?=ahead)', NOFOLD(u'lookahead'), 0) = u'look';
NOT(REGEXFIND(u'.ook(?=ahead)', NOFOLD(u'bookmarks')));
NOT(REGEXFIND(u'.ook(?!ahead)', NOFOLD(u'lookahead')));
REGEXFIND(u'.ook(?!ahead)', NOFOLD(u'bookmarks'));
REGEXFIND(u'.ook(?!ahead)', NOFOLD(u'bookmarks'), 0) = u'book';

REGEXFIND(u8's.t', NOFOLD(u8'set'));
NOT(REGEXFIND(u8's.t', NOFOLD(u8'select')));
REGEXFIND(u8's.*t', NOFOLD(u8'select'));
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'upset sot resets it should match'));
NOT(REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE')));
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), NOCASE);
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), 0, NOCASE) = u8'UPSET SOT RESETS IT ';
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), 1, NOCASE) = u8'RESETS IT ';
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), 2, NOCASE) = u8'RE';
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), 3, NOCASE) = u8'S IT';
REGEXFIND(u8'^((..)?(s ?.t)* )+', NOFOLD(u8'UPSET SOT RESETS IT should match only with NOCASE'), 4, NOCASE) = u8'';
REGEXFIND(u8'.ook(?=ahead)', NOFOLD(u8'lookahead'));
REGEXFIND(u8'.ook(?=ahead)', NOFOLD(u8'lookahead'), 0) = u8'look';
NOT(REGEXFIND(u8'.ook(?=ahead)', NOFOLD(u8'bookmarks')));
NOT(REGEXFIND(u8'.ook(?!ahead)', NOFOLD(u8'lookahead')));
REGEXFIND(u8'.ook(?!ahead)', NOFOLD(u8'bookmarks'));
REGEXFIND(u8'.ook(?!ahead)', NOFOLD(u8'bookmarks'), 0) = u8'book';
