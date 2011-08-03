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
export checkMatch(got, expected) :=
    macro
        output(IF(got = expected, 'OK', 'Fail: Got(' + got + ') Expected(' + expected + ')'))
    endmacro;

export checkUMatch(got, expected) :=
    macro
        output(IF(got = expected, U'OK', U'Fail: Got(' + got + U') Expected(' + expected + U')'))
    endmacro;

//-- Strings --//
checkMatch('One', 'One');
checkMatch(XMLENCODE('One'), 'One');
checkMatch(XMLENCODE('<One name="x" value=\'xx\'>'), '&lt;One name=&quot;x&quot; value=&apos;xx&apos;&gt;');
checkMatch(XMLDECODE(XMLENCODE('<One name="x" value=\'xx\'>')), '<One name="x" value=\'xx\'>');
checkMatch(XMLENCODE('<One name="x" value=\'xx\'>\nzzz</One>'), '&lt;One name=&quot;x&quot; value=&apos;xx&apos;&gt;\nzzz&lt;/One&gt;');
checkMatch(XMLENCODE('<One name="x" value=\'xx\'>\nzzz</One>',ALL), '&lt;One&#32;name=&quot;x&quot;&#32;value=&apos;xx&apos;&gt;&#10;zzz&lt;/One&gt;');

checkMatch(XMLDECODE('&#65;&#x42;&lt;&#9;'),'AB<\t');
checkMatch(XMLENCODE(XMLDECODE('&#65;&#x42;&lt;&#9;'),ALL),'AB&lt;&#9;');
checkMatch(XMLENCODE('\a\b\f\n\r\t\v\?\'\"',ALL), '\a\b\f&#10;&#13;&#9;\v\?&apos;&quot;');

//-- Unicode --//
checkUMatch(U'One', U'One');
checkUMatch(XMLENCODE(U'One'), U'One');
checkUMatch(XMLENCODE(U'<One name="x" value=\'xx\'>'), U'&lt;One name=&quot;x&quot; value=&apos;xx&apos;&gt;');
checkUMatch(XMLDECODE(XMLENCODE(U'<One name="x" value=\'xx\'>')), U'<One name="x" value=\'xx\'>');
checkUMatch(XMLENCODE(U'<One name="x" value=\'xx\'>\nzzz</One>'), U'&lt;One name=&quot;x&quot; value=&apos;xx&apos;&gt;\nzzz&lt;/One&gt;');
checkUMatch(XMLENCODE(U'<One name="x" value=\'xx\'>\nzzz</One>',ALL), U'&lt;One&#32;name=&quot;x&quot;&#32;value=&apos;xx&apos;&gt;&#10;zzz&lt;/One&gt;');

checkUMatch(XMLDECODE(U'&#65;&#x42;&lt;&#9;'),U'AB<\t');
checkUMatch(XMLENCODE(XMLDECODE(U'&#65;&#x42;&lt;&#9;'),ALL),U'AB&lt;&#9;');

checkUMatch(XMLDECODE(U'�?�?�?ਲ੬ઊ੨ਲਭ!&#x2654;&#x55AE;&#x55af;'),U'�?�?�?ਲ੬ઊ੨ਲਭ!♔單喯');
checkUMatch(XMLENCODE(XMLDECODE(U'�?�?�?ਲ੬ઊ੨ਲਭ!&#x2654;&#x55AE;&#x55af;'),ALL),U'�?�?�?ਲ੬ઊ੨ਲਭ!♔單喯');
checkUMatch(XMLENCODE(U'\a\b\f\n\r\t\v\?\'\"',ALL), U'\a\b\f&#10;&#13;&#9;\v\?&apos;&quot;');
