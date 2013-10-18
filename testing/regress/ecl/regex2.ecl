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

#option ('globalFold', false)
REGEXFIND(NOFOLD('s.t'), 'set');
NOT(REGEXFIND(NOFOLD('s.t'), 'select'));
REGEXFIND(NOFOLD('s.*t'), 'select');
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'upset sot resets it should match');
NOT(REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE'));
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', NOCASE);
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', 0, NOCASE) = 'UPSET SOT RESETS IT ';
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', 1, NOCASE) = 'RESETS IT ';
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', 2, NOCASE) = 'RE';
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', 3, NOCASE) = 'S IT';
REGEXFIND(NOFOLD('^((..)?(s ?.t)* )+'), 'UPSET SOT RESETS IT should match only with NOCASE', 4, NOCASE) = '';
REGEXFIND(NOFOLD('.ook(?=ahead)'), 'lookahead');
REGEXFIND(NOFOLD('.ook(?=ahead)'), 'lookahead', 0) = 'look';
NOT(REGEXFIND(NOFOLD('.ook(?=ahead)'), 'bookmarks'));
NOT(REGEXFIND(NOFOLD('.ook(?!ahead)'), 'lookahead'));
REGEXFIND(NOFOLD('.ook(?!ahead)'), 'bookmarks');
REGEXFIND(NOFOLD('.ook(?!ahead)'), 'bookmarks', 0) = 'book';

REGEXFIND(NOFOLD(u's.t'), u'set');
NOT(REGEXFIND(NOFOLD(u's.t'), u'select'));
REGEXFIND(NOFOLD(u's.*t'), u'select');
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'upset sot resets it should match');
NOT(REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE'));
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', NOCASE);
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', 0, NOCASE) = u'UPSET SOT RESETS IT ';
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', 1, NOCASE) = u'RESETS IT ';
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', 2, NOCASE) = u'RE';
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', 3, NOCASE) = u'S IT';
REGEXFIND(NOFOLD(u'^((..)?(s ?.t)* )+'), u'UPSET SOT RESETS IT should match only with NOCASE', 4, NOCASE) = u'';
REGEXFIND(NOFOLD(u'.ook(?=ahead)'), u'lookahead');
REGEXFIND(NOFOLD(u'.ook(?=ahead)'), u'lookahead', 0) = u'look';
NOT(REGEXFIND(NOFOLD(u'.ook(?=ahead)'), u'bookmarks'));
NOT(REGEXFIND(NOFOLD(u'.ook(?!ahead)'), u'lookahead'));
REGEXFIND(NOFOLD(u'.ook(?!ahead)'), u'bookmarks');
REGEXFIND(NOFOLD(u'.ook(?!ahead)'), u'bookmarks', 0) = u'book';

