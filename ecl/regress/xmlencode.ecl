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
