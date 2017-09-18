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

#OPTION('writeInlineContent', true);

import lib_stringlib;
import std.str;
import std.system.thorlib;

nameRecord := RECORD
    STRING name;
END;

houseRecord := RECORD
    UNSIGNED id;
    DATASET(nameRecord) names;
END;

STRING getTempFilename1() := BEGINC++
#include <stdio.h>
#body
    char buffer[TMP_MAX];
    const char * name = tmpnam(buffer);
    size32_t len = (size32_t)strlen(name);
    void * data = rtlMalloc(len);
    memcpy(data, name, len);
    __lenResult = len;
    __result = (char *)data;
ENDC++;

STRING getTempFilename2() := 'temp' + (string)RANDOM() + '.tmp';
STRING getTempFilename3() := 'temp' + (string)RANDOM() + '.tmp';
STRING getTempFilename4() := 'temp' + (string)RANDOM() + '.tmp';

tempname1 := getTempFilename2() : INDEPENDENT;
tempname2 := getTempFilename3() : INDEPENDENT;
tempname3 := getTempFilename4() : INDEPENDENT;

tempname1s := tempname1 + thorlib.node();
tempname2s := tempname2 + thorlib.node();
tempname3s := tempname3 + thorlib.node();

#IF (__OS__ = 'windows')
  STRING catCmd(STRING filename) := 'cmd /C cat > ' + filename;
#ELSE
  STRING catCmd(STRING filename) := 'bash -c "cat > ' + filename + '"';
#END

d1 := DATASET([{1,[{'Gavin'},{'John'}]},{2,[{'Steve'},{'Steve'},{'Steve'}]},{3,[]}], houseRecord);

//d1 := dataset(['</Dataset>', ' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>', '    <Dataset>' ], { string line }) : stored('nofold');
d2 := DATASET([' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>'], { string line }) : stored('nofold2');
d3 := DATASET([' <Dataset><Row>Line3</Row></Dataset>', '  <Dataset><Row>Middle</Row></Dataset>', '   <Dataset><Row>Line1</Row></Dataset>'], { string line }) : stored('nofold3');

d4 := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { string line}) : stored('nofold');

p1 := PIPE(d1, 'cat', houseRecord);
p2 := PIPE(d1, 'cat', houseRecord, xml(noroot), output(xml(noroot)));
p3 := PIPE(d1, 'cat', { STRING line }, csv, output(xml(noroot)));

OUTPUT(p1);
OUTPUT(p2);
OUTPUT(p3, { string l := Str.FindReplace(line, '\r', ' ') } );

csvRec := { STRING lout; };

SEQUENTIAL(
 PARALLEL(
  OUTPUT(d1,,PIPE(catCmd(tempname1s))),
  OUTPUT(d4,,PIPE(catCmd(tempname2s), csv)),
  OUTPUT(d1,,PIPE(catCmd(tempname3s), xml(noroot)))
 ),
 PARALLEL(
  OUTPUT(PIPE('sort ' + tempname2s, csvRec, csv)),
  OUTPUT(PIPE('cat ' + tempname3s, houseRecord, xml('Dataset/Row')))
 )
);

inlineHouseRecord := RECORD
    UNSIGNED id;
    STRING names {XPATH('names/<>')};
END;

inline1 := DATASET([{1,'<Row><name>Gavin</name></Row><Row><name>John</name></Row>'},{2,'<Row><name>Steve</name></Row><Row><name>Steve</name></Row>'},{3,''}], inlineHouseRecord);

pin1 := PIPE(inline1, 'cat', houseRecord, xml(noroot), output(xml(noroot)));
OUTPUT(pin1, NAMED('inlineXML'));
