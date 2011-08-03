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

INTEGER MaxTxtLength := 1000000;

layout_doc := RECORD
    STRING line{maxlength(MaxTxtLength)};
END;

//doc := DATASET('~thor::adTEMP::htmlsample3', layout_doc, CSV(SEPARATOR(''), TERMINATOR('<@@jjfiefsiur@@>'), QUOTE(''), MAXLENGTH(MaxTxtLength)));

doc := dataset([{'<td VALIGN=top ALIGN=left><FONT FACE="TIMES NEW ROMAN, TIMES"> &nbsp; &nbsp; &nbsp; &nbsp;SUPPORTERS OF MAKING the ban on Internet access taxes permanent have prevailed in the House, and an identical bill has passed a Senate committee and could be put to the full Senate as early as this week.<BR> &nbsp; &nbsp; &nbsp; &nbsp;For proponents, the issue is simple: Internet use is vital to economic growth, and going online should not be subject to a new tax when consumers already pay an array of local and state taxes and fees for telephone and cable television services. <BR> &nbsp; &nbsp; &nbsp; &nbsp;That view was backed by Congress five years ago when it first approved the ban. As a result, states cannot levy taxes or impose fees on Internet service providers, such as America Online or EarthLink, for providing consumers with Internet accounts.<BR> &nbsp; &nbsp; &nbsp; &nbsp;But those were the days when most consumers went online via ordinary phone lines, sometimes paying for additional lines. Although no tax could be levied on the Internet account, a consumer still paid taxes and fees for use of a second phone line. <BR> &nbsp; &nbsp; &nbsp; &nbsp;Now, increasing numbers of consumers are going online via high-speed systems that piggyback on existing lines, rendering extra lines unnecessary. Digital-subscriber-line service, for example, provided over phone lines, is in about 9 million U.S. homes. <BR> &nbsp; &nbsp; &nbsp; &nbsp;Because the same phone line carries voice calls and data for Internet use, many states and localities have taxed DSL services. EarthLink, for example, collects taxes in 25 states and the District at the behest of local governments. </FONT> </TD> <TD WIDTH=53> </TD> </TR> <TR>'}], layout_doc);



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
PATTERN atrash := PATTERN('[^/>]+'); //REPEAT(ANY NOT IN ['/', '>'], 1, ANY, MIN);
PATTERN etago := lt ws* tagname attribute* ws* atrash* opt('/' ws*) gt;
PATTERN etagc := lt ws* '/' ws* tagname ws* gt;
PATTERN html_text := PATTERN('.*?'); //PATTERN('[^<]*'); //(ANY NOT IN lt)*;
pattern html := html_text (etago | etagc | comment);

SetIgnoreTags := ['script', 'style', 'select'];
SetBlockTags := ['p', 'div', 'td', 'th', 'table', 'br', 'hr', 'li', 'ul', 'ol', 'dt', 'pre', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'center', 'blockquote'];
SetEndSentence := ['.', '?', '!', ':'];

STRING entityclean(STRING txt) :=
    stringlib.StringFindReplace(
     stringlib.StringFindReplace(
      stringlib.StringFindReplace(txt, '&nbsp;', ' '),
                                      '&amp;', '&'),
                                     '&copy;', '(C)');
layout_html := RECORD
//  UNSIGNED8 docid := a.docid;
    STRING20 tag := IF(MATCHED(etago), MATCHTEXT(etago/tagname), MATCHTEXT(etagc/tagname));
    STRING htmltext{MAXLENGTH(MaxTxtLength)} := stringlib.stringsubstituteout(MATCHTEXT(html_text), '\r\n\t', ' ');
    STRING htmltext2{MAXLENGTH(MaxTxtLength)} := MATCHTEXT(html_text);
    unsigned4 p := matchposition(html_text);
    unsigned4 l := matchlength(html_text);
END;

r := PARSE(doc, line, html, layout_html, SCAN);
OUTPUT(r);

