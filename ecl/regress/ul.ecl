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
import lib_unicodelib;

unicodelib.UnicodeFilterOut(u'abcde', u'cd') = u'abe';
unicodelib.UnicodeFilter(u'abcdeabcdec', u'cd') = u'cdcdc';
unicodelib.UnicodeSubstituteOut(u'abcde', u'cd', u'x') = u'abxxe';
unicodelib.UnicodeSubstitute(u'abcdeabcdec', u'cd', u'x') = u'xxcdxxxcdxc';

unicodelib.UnicodeRepad(u' abc', 6) + u'd' = u'abc   d';

unicodelib.UnicodeFind(u'abcdcdef', u'cd', 1) = 3;
unicodelib.UnicodeFind(u'abcdcdef', u'cd', 2) = 5;
unicodelib.UnicodeFind(u'abcdcdef', u'cd', 3) = 0;
unicodelib.UnicodeLocaleFind(u'x\u0131zxyz', u'y', 1, 'lt') = 2; // U+0131 is lowercase I w/o dot, which equals y in Lithuanian...
unicodelib.UnicodeLocaleFind(u'x\u0131zxyz', u'y', 1, 'tr') = 5; // ...but not in Turkish
base := u'caf\u00E9';                                    // U+00E9 is lowercase E with acute
prim := u'coffee shop';                                  // primary difference, different letters
seco := u'cafe';                                         // secondary difference, different accents (missing base's acute)
tert := u'Caf\u00C9';                                    // tertiary difference, different capitalization (U+00C9 is u/c E + acute)
search := seco + tert + base;
unicodelib.UnicodeLocaleFindAtStrength(search, base, 1, 'fr', 1) = 1; // at strength 1, base matches seco (only secondary diffs)
unicodelib.UnicodeLocaleFindAtStrength(search, base, 1, 'fr', 2) = 5; // at strength 2, base matches tert (only tertiary diffs)
unicodelib.UnicodeLocaleFindAtStrength(search, base, 1, 'fr', 3) = 9; // at strength 3, base doesn't match either seco or tert
unicodelib.UnicodeLocaleFindAtStrength(u'le caf\u00E9 vert', u'cafe', 1, 'fr', 2) = 4; // however, an accent on the source,
unicodelib.UnicodeLocaleFindAtStrength(u'le caf\u00E9 vert', u'cafe', 1, 'fr', 3) = 4; // rather than on the pattern,
unicodelib.UnicodeLocaleFindAtStrength(u'le caf\u00E9 vert', u'cafe', 1, 'fr', 4) = 4; // is ignored at strengths up to 4,
unicodelib.UnicodeLocaleFindAtStrength(u'le caf\u00E9 vert', u'cafe', 1, 'fr', 5) = 0; // and only counts at strength 5

unicodelib.UnicodeExtract(u'a,b,c', 1) = u'a';
unicodelib.UnicodeExtract(u'a,b,c', 2) = u'b';
unicodelib.UnicodeExtract(u'a,b,c', 3) = u'c';
unicodelib.UnicodeExtract(u'a,b,c', 4) = u'';
unicodelib.UnicodeExtract50(u'a,b,c', 1) = u'a';
unicodelib.UnicodeExtract50(u'a,b,c', 2) = u'b';
unicodelib.UnicodeExtract50(u'a,b,c', 3) = u'c';
unicodelib.UnicodeExtract50(u'a,b,c', 4) = u'';

unicodelib.UnicodeToLowerCase(u'ABcde') = u'abcde';
unicodelib.UnicodeToUpperCase(u'ABcde i') = u'ABCDE I';
unicodelib.UnicodeToProperCase(u'abCDe fGhIJ') = u'Abcde Fghij';
unicodelib.UnicodeToLowerCase80(u'ABcde') = u'abcde';
unicodelib.UnicodeToUpperCase80(u'ABcde') = u'ABCDE';
unicodelib.UnicodeToProperCase80(u'ABcde') = u'Abcde';
unicodelib.UnicodeLocaleToUpperCase(u'hij', 'tr') = u'H\u0130J'; // U+0130 is uppercase I with dot, which is uc for i in Turkish

unicodeLib.UnicodeCompareAtStrength(base, prim, 1) != 0; // base and prim differ at all strengths
unicodeLib.UnicodeCompareAtStrength(base, seco, 1) = 0;  // base and seco same at strength 1 (differ only at strength 2)
unicodeLib.UnicodeCompareAtStrength(base, tert, 1) = 0;  // base and tert same at strength 1 (differ only at strength 3)
unicodeLib.UnicodeCompareAtStrength(base, base, 1) = 0;  // base always same as itself
unicodeLib.UnicodeCompareAtStrength(base, prim, 2) != 0; // base and prim differ at all strengths
unicodeLib.UnicodeCompareAtStrength(base, seco, 2) != 0; // base and seco differ at strength 2
unicodeLib.UnicodeCompareAtStrength(base, tert, 2) = 0;  // base and tert same at strength 3 (differ only at strength 3)
unicodeLib.UnicodeCompareAtStrength(base, base, 2) = 0;  // base always same as itself
unicodeLib.UnicodeCompareAtStrength(base, prim, 3) != 0; // base and prim differ at all strengths
unicodeLib.UnicodeCompareAtStrength(base, seco, 3) != 0; // base and seco differ at strength 2
unicodeLib.UnicodeCompareAtStrength(base, tert, 3) != 0; // base and tert differ at strength 3
unicodeLib.UnicodeCompareAtStrength(base, base, 3) = 0;  // base always same as itself
unicodeLib.UnicodeCompareIgnoreCase(u'this', u'THIS') = 0;
unicodeLib.UnicodeCompareIgnoreCase(u'this', u'THAT') != 0;

ltbase := u'\u00E9\u0131'; // lowercase E with acute, lowercase I w/o dot
ltseco := u'Ey';           // uppercase E, lowercase y - secondary difference in Lithuanian
lttert := u'\u00C9y';      // uppercase E with acute, lowercase y - tertiary difference in Lithuanian
unicodeLib.UnicodeLocaleCompareAtStrength(ltbase, ltseco, 'lt', 1) = 0;  // in Lithuanian, ltbase and ltseco differ at strength 2
unicodeLib.UnicodeLocaleCompareAtStrength(ltbase, ltseco, 'lt', 2) != 0; // in Lithuanian, ltbase and ltseco differ at strength 2
unicodeLib.UnicodeLocaleCompareAtStrength(ltbase, lttert, 'lt', 2) = 0;  // in Lithuanian, ltbase and lttert differ at strength 3
unicodeLib.UnicodeLocaleCompareAtStrength(ltbase, lttert, 'lt', 3) != 0; // in Lithuanian, ltbase and lttert differ at strength 3
unicodeLib.UnicodeLocaleCompareAtStrength(ltbase, ltseco, 'tr', 1) != 0; // in Turkish, ltbase and ltseco differ at all strengths

unicodeLib.UnicodeReverse(u'mirror') = u'rorrim';

unicodelib.UnicodeFindReplace(u'abcde', u'a', u'AAAAA') = u'AAAAAbcde';
unicodelib.UnicodeFindReplace(u'aaaaa', u'aa', u'b') = u'bba';
unicodelib.UnicodeFindReplace(u'aaaaaa', u'aa', u'b') = u'bbb';
unicodelib.UnicodeFindReplace80(u'aaaaaa', u'aa', u'b') = u'bbb';
unicodelib.UnicodeLocaleFindReplace(u'gh\u0131klm', u'hyk', u'XxXxX', 'lt') = u'gXxXxXlm';
unicodelib.UnicodeLocaleFindReplace(u'gh\u0131klm', u'hyk', u'X', 'lt') = u'gXlm';
unicodelib.UnicodeLocaleFindReplace80(u'gh\u0131klm', u'hyk', u'X', 'lt') = u'gXlm';
unicodelib.UnicodeLocaleFindAtStrengthReplace(u'e\u00E8E\u00C9eE', u'e\u00E9', u'xyz', 'fr', 1) = u'xyzxyzxyz';
unicodelib.UnicodeLocaleFindAtStrengthReplace(u'e\u00E8E\u00C9eE', u'e\u00E9', u'xyz', 'fr', 2) = u'e\u00E8xyzeE';
unicodelib.UnicodeLocaleFindAtStrengthReplace(u'e\u00E8E\u00C9eE', u'e\u00E9', u'xyz', 'fr', 3) = u'e\u00E8E\u00C9eE';
unicodelib.UnicodeLocaleFindAtStrengthReplace80(u'e\u00E8E\u00C9eE', u'e\u00E9', u'xyz', 'fr', 3) = u'e\u00E8E\u00C9eE';

u'ab' + unicodelib.UnicodeCleanSpaces(u'  cd  ef  gh  ') + u'ij' = u'abcd ef ghij';
unicodelib.UnicodeCleanSpaces25(u'  cd  ef  gh  ') = u'cd ef gh';
unicodelib.UnicodeCleanSpaces80(u'  cd  ef  gh  ') = u'cd ef gh';

unicodelib.UnicodeContains(u'the quick brown fox jumps over the lazy dog', u'abcdefghijklmnopqrstuvwxyz', false);
NOT(unicodelib.UnicodeContains(u'the speedy ochre vixen leapt over the indolent retriever', u'abcdefghijklmnopqrstuvwxyz', false));
NOT(unicodelib.UnicodeContains(u'the quick brown fox jumps over the lazy dog', u'ABCdefghijklmnopqrstuvwxyz', false));
unicodelib.UnicodeContains(u'the quick brown fox jumps over the lazy dog', u'ABCdefghijklmnopqrstuvwxyz', true);

