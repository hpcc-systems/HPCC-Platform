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
//these should be const-folded, and give 'the cap sap on the map'
REGEXREPLACE(NOFOLD('(.a)t'), 'the cat sat on the mat', '$1p');
REGEXREPLACE(NOFOLD(u'(.a)t'), u'the cat sat on the mat', u'$1p');

inrec := RECORD
    STRING10 str;
    UNICODE10 ustr;
END;

inset := nofold(DATASET([{'She', u'Eins'}, {'Sells', u'Zwei'}, {'Sea', u'Drei'}, {'Shells', u'Vier'}], inrec));

outrec := RECORD
    STRING10 orig;
    STRING10 withcase;
    STRING10 wocase;
    UNICODE10 uorig;
    UNICODE10 uwithcase;
    UNICODE10 uwocase;
END;

outrec trans(inrec l) := TRANSFORM
    SELF.orig := l.str;
    SELF.withcase := REGEXREPLACE('s', l.str, 'f');
    SELF.wocase := REGEXREPLACE('s', l.str, 'f', NOCASE);
    SELF.uorig := l.ustr;
    SELF.uwithcase := REGEXREPLACE(u'e', l.ustr, u'\u00EB');
    SELF.uwocase := REGEXREPLACE(u'e', l.ustr, u'\u00EB', NOCASE);
END;

outset := PROJECT(inset, trans(LEFT));

output(outset);

