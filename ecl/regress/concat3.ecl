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

import text;
layout_doc := RECORD
    STRING line{maxlength(50000)};
END;

a := DATASET('~thor::adTEMP::htmlsample', layout_doc, CSV(SEPARATOR(''), TERMINATOR('</HTML>'), QUOTE(''), MAXLENGTH(50000)));

PATTERN ws := text.ws;
PATTERN lt := '<';
PATTERN tagname := text.word;
PATTERN attname := text.word;
PATTERN gt := '>';
PATTERN quoteds := '\'' PATTERN('[^\']*') '\'';
PATTERN quotedd := '"' PATTERN('[^"]*') '"';
PATTERN attval := quoteds | quotedd | PATTERN('[^ \r\n\t>]*');
PATTERN attribute := ws attname ws* opt('=' ws* attval);
PATTERN comment := '<!--' REPEAT(ANY, 0, ANY, MIN) '-->';
PATTERN etago := lt ws* tagname attribute* ws* opt('/' ws*) gt;
PATTERN etagc := lt ws* '/' ws* tagname ws* gt;
PATTERN trailing_space := text.ws;
PATTERN html_text := (ANY NOT IN lt)*;
RULE html := html_text (etago | etagc | comment);

layout_html := RECORD
    STRING20 tag := IF(MATCHED(etago), MATCHTEXT(etago/tagname), MATCHTEXT(etagc/tagname));
    STRING htmltext{MAXLENGTH(5000)} := MATCHTEXT(html_text); //stringlib.stringsubstituteout(MATCHTEXT(html_text), '\r\n\t', ' ');
END;

r := PARSE(a, line, html, layout_html, SCAN);
OUTPUT(r);

layout_p := RECORD
    BOOLEAN space_at_end := FALSE;
    STRING txt{MAXLENGTH(5000000)} := '';
END;

layout_p DoProject(layout_html le) := TRANSFORM
    SELF.txt := TRIM(le.htmltext);
    SELF.space_at_end := false; //LENGTH(self.txt) != LENGTH(le.htmltext);
END;

var := PROJECT(r, DoProject(LEFT));
OUTPUT(var);

layout_p Concatenate(layout_p le, layout_p ri) := TRANSFORM
    SELF.txt := le.txt +  
            IF(le.space_at_end, 
                IF(ri.txt = '', '', ri.txt),
                ri.txt);
END;

fr := ROLLUP(var, true, Concatenate(LEFT, RIGHT));
OUTPUT(fr);
