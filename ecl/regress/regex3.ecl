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

#option ('globalFold', false);
REGEXFIND('s.t', 'set');
NOT(REGEXFIND('s.t', 'select'));
REGEXFIND('s.*t', 'select');
REGEXFIND('^((..)?(s ?.t)* )+', 'upset sot resets it should match');
NOT(REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE'));
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', NOCASE);
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', 0, NOCASE) = 'UPSET SOT RESETS IT ';
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', 1, NOCASE) = 'RESETS IT ';
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', 2, NOCASE) = 'RE';
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', 3, NOCASE) = 'S IT';
REGEXFIND('^((..)?(s ?.t)* )+', 'UPSET SOT RESETS IT should match only with NOCASE', 4, NOCASE) = '';
REGEXFIND('.ook(?=ahead)', 'lookahead');
REGEXFIND('.ook(?=ahead)', 'lookahead', 0) = 'look';
NOT(REGEXFIND('.ook(?=ahead)', 'bookmarks'));
NOT(REGEXFIND('.ook(?!ahead)', 'lookahead'));
REGEXFIND('.ook(?!ahead)', 'bookmarks');
REGEXFIND('.ook(?!ahead)', 'bookmarks', 0) = 'book';

REGEXFIND(u's.t', u'set');
NOT(REGEXFIND(u's.t', u'select'));
REGEXFIND(u's.*t', u'select');
REGEXFIND(u'^((..)?(s ?.t)* )+', u'upset sot resets it should match');
NOT(REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE'));
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', NOCASE);
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', 0, NOCASE) = u'UPSET SOT RESETS IT ';
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', 1, NOCASE) = u'RESETS IT ';
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', 2, NOCASE) = u'RE';
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', 3, NOCASE) = u'S IT';
REGEXFIND(u'^((..)?(s ?.t)* )+', u'UPSET SOT RESETS IT should match only with NOCASE', 4, NOCASE) = u'';
REGEXFIND(u'.ook(?=ahead)', u'lookahead');
REGEXFIND(u'.ook(?=ahead)', u'lookahead', 0) = u'look';
NOT(REGEXFIND(u'.ook(?=ahead)', u'bookmarks'));
NOT(REGEXFIND(u'.ook(?!ahead)', u'lookahead'));
REGEXFIND(u'.ook(?!ahead)', u'bookmarks');
REGEXFIND(u'.ook(?!ahead)', u'bookmarks', 0) = u'book';

