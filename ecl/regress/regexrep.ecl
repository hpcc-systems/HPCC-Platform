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
//these should be const-folded, and give 'the cap sap on the map'
REGEXREPLACE('(.a)t', 'the cat sat on the mat', '$1p');
REGEXREPLACE(u'(.a)t', u'the cat sat on the mat', u'$1p');

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

