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

#option ('globalFold', false);

//these should be const-folded, and give 'the cap sap on the map'
REGEXREPLACE(NOFOLD('(.a)t'), 'the cat sat on the mat', '$1p');
REGEXREPLACE(NOFOLD(u'(.a)t'), u'the cat sat on the mat', u'$1p');
REGEXREPLACE(NOFOLD(u8'(.a)t'), u8'the cat sat on the mat', u8'$1p');

inrec := RECORD
    STRING10 str;
    UNICODE10 ustr;
    UTF8 u8str;
END;

inset := nofold(DATASET([{'She', u'Eins', u8'Eins'},
                         {'Sells', u'Zwei', u8'Zwei'},
                         {'Sea', u'Drei', u8'Drei'},
                         {'Shells', u'Vier', u8'Vier'}], inrec));

outrec := RECORD
    STRING10 orig;
    STRING10 withcase;
    STRING10 wocase;
    UNICODE10 uorig;
    UNICODE10 uwithcase;
    UNICODE10 uwocase;
    UTF8 u8orig;
    UTF8 u8withcase;
    UTF8 u8wocase;
END;

outrec trans(inrec l) := TRANSFORM
    SELF.orig := l.str;
    SELF.withcase := REGEXREPLACE('s', l.str, 'f');
    SELF.wocase := REGEXREPLACE('s', l.str, 'f', NOCASE);
    SELF.uorig := l.ustr;
    SELF.uwithcase := REGEXREPLACE(u'e', l.ustr, u'\u00EB');
    SELF.uwocase := REGEXREPLACE(u'e', l.ustr, u'\u00EB', NOCASE);
    SELF.u8orig := l.u8str;
    SELF.u8withcase := REGEXREPLACE(u8'e', l.u8str, u8'ë');
    SELF.u8wocase := REGEXREPLACE(u8'e', l.u8str, u8'ë', NOCASE);
END;

outset := PROJECT(inset, trans(LEFT));

output(outset);

// HPCC-31954
REGEXREPLACE('\'', 'Dan\'s', '\\\'') = 'Dan\'s';
REGEXREPLACE(NOFOLD('\''), 'Dan\'s', '\\\'') = 'Dan\'s';
REGEXREPLACE(u'\'', u'Dan\'s', u'\\\'') = u'Dan\'s';
REGEXREPLACE(NOFOLD(u'\''), u'Dan\'s', u'\\\'') = u'Dan\'s';

// HPCC-32461
REGEXREPLACE('\\b(N)EW (M)EXICO\\b|\\b(U)NITED (S)TATES\\b','NEW MEXICO','$1$2$3');
REGEXREPLACE('(\\w+)', 'JustOneWord', '$2 $1');
