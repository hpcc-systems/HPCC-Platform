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
