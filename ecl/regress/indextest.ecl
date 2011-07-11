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

baserec := RECORD
    STRING6 name;
    INTEGER6 blah;
    STRING9 value;
END;

baseset := DATASET([{'fruit', 123, 'apple'}, {'music', 456, 'aphex'}, {'os', 789, 'linux'}, {'car', 246, 'ford'}, {'os', 468, 'win32'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'book', 987, 'cch22'}], baserec);

genbase := OUTPUT(baseset, , '~bug15197.d00', OVERWRITE);

fpbaserec := RECORD
    baserec;
    UNSIGNED8 filepos{virtual(fileposition)};
END;

fpbaseset := DATASET('~bug15197.d00', fpbaserec, FLAT);

genidx := BUILDINDEX(fpbaseset, {name, filepos}, '~bug15197.idx', OVERWRITE);

goodidx := INDEX(fpbaseset, {name, filepos}, '~bug15197.idx');

badidx := INDEX(fpbaseset, {blah, filepos}, '~bug15197.idx');

SEQUENTIAL(genbase, genidx, OUTPUT(goodidx), OUTPUT(badidx));

