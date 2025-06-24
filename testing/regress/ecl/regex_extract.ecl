/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

// HPCC-34419

PersonRecStr := RECORD
    STRING name;
    STRING age;
    STRING title;
    STRING other;
END;

s1 := 'id=1001, name="Dan Camper", supervisor="Gavin", title="Architect"' : STORED('x');

r1 := REGEXEXTRACT('(name="(.*?)",?)', s1, NOCASE);
r2 := REGEXEXTRACT('(age=(\\d+),?)', r1[1], NOCASE);
r3 := REGEXEXTRACT('(title="(.*?)",?)', r2[1], NOCASE);

foundName1 := r1[3];
foundAge1 := r2[3];
foundTitle1 := r3[3];
other1 := TRIM(r3[1], LEFT, RIGHT);

result1 := DATASET
    (
        [{foundName1, foundAge1, foundTitle1, other1}],
        PersonRecStr
    );

OUTPUT(result1, NAMED('result_1'));

//--------------------------------------------

PersonRecU8Str := RECORD
    UTF8 name;
    UTF8 age;
    UTF8 title;
    UTF8 other;
END;

s2 := u8'id=1001, name="Renée Åström", supervisor="Gavin", title="Développeur sénior 💻"' : STORED('y');

r2_1 := REGEXEXTRACT(u8'(name="(.*?)",?)', s2, NOCASE);
r2_2 := REGEXEXTRACT(u8'(age=(\\d+),?)', r2_1[1], NOCASE);
r2_3 := REGEXEXTRACT(u8'(title="(.*?)",?)', r2_2[1], NOCASE);

foundName2 := r2_1[3];
foundAge2 := r2_2[3];
foundTitle2 := r2_3[3];
other2 := TRIM(r2_3[1], LEFT, RIGHT);

result2 := DATASET
    (
        [{foundName2, foundAge2, foundTitle2, other2}],
        PersonRecU8Str
    );

OUTPUT(result2, NAMED('result_2'));

//--------------------------------------------

PersonRecUStr := RECORD
    UNICODE name;
    UNICODE age;
    UNICODE title;
    UNICODE other;
END;

s3 := u'id=1001, name="李小龙", supervisor="Gavin", title="武术教练"' : STORED('z');

r3_1 := REGEXEXTRACT(u'(name="(.*?)",?)', s3, NOCASE);
r3_2 := REGEXEXTRACT(u'(age=(\\d+),?)', r3_1[1], NOCASE);
r3_3 := REGEXEXTRACT(u'(title="(.*?)",?)', r3_2[1], NOCASE);

foundName3 := r3_1[3];
foundAge3 := r3_2[3];
foundTitle3 := r3_3[3];
other3 := TRIM(r3_3[1], LEFT, RIGHT);

result3 := DATASET
    (
        [{foundName3, foundAge3, foundTitle3, other3}],
        PersonRecUStr
    );

OUTPUT(result3, NAMED('result_3'));
