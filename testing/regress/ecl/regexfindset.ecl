/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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


/* ascii strings */
str := 'add-subtract 123.90, or 123';
REGEXFINDSET(NOFOLD('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)'), str);

REGEXFINDSET('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', str);

/* Unicode strings */
ustr := U'add-subtract 123.90, or 123';
REGEXFINDSET(NOFOLD(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)'), ustr);

REGEXFINDSET(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', ustr);

/* UTF-8 strings */
u8str := U8'add-subtract 123.90, or 123';
REGEXFINDSET(NOFOLD(U8'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)'), u8str);

REGEXFINDSET(U8'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', u8str);

/* in Project test */
RecLayout := RECORD
    STRING50 rawstr;
END;
ParsedLayout := RECORD
    SET OF STRING parsed;
END;

ParsedLayout parseThem(recLayout L) := TRANSFORM
    SELF.parsed := REGEXFINDSET('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', l.rawstr);
END;

recTable := dataset([
    {'subtract 35 from 50'},
    {'divide 100 by 25 and add 5'}
    ], RecLayout);

PROJECT(recTable, parseThem(LEFT));

recTable2 := NOFOLD(dataset([
    {'subtract 35 from 50'},
    {'divide 100 by 25 and add 5'}
    ], RecLayout));

PROJECT(recTable2, parseThem(LEFT));

URecLayout := RECORD
    UNICODE50 rawstr;
END;
UParsedLayout := RECORD
    SET OF UNICODE parsed;
END;

UParsedLayout uparseThem(URecLayout L) := TRANSFORM
    SELF.parsed := REGEXFINDSET(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', l.rawstr);
END;

urecTable := dataset([
    {U'subtract 35 from 50'},
    {U'divide 100 by 25 and add 5'}
    ], URecLayout);

PROJECT(urecTable, uparseThem(LEFT));

urecTable2 := NOFOLD(dataset([
    {U'subtract 35 from 50'},
    {U'divide 100 by 25 and add 5'}
    ], URecLayout));

PROJECT(urecTable2, uparseThem(LEFT));

U8RecLayout := RECORD
    UTF8 rawstr;
END;
U8ParsedLayout := RECORD
    SET OF UTF8 parsed;
END;

U8ParsedLayout u8parseThem(U8RecLayout L) := TRANSFORM
    SELF.parsed := REGEXFINDSET(U8'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', l.rawstr);
END;

u8recTable := dataset([
    {U8'subtract 35 from 50'},
    {U8'divide 100 by 25 and add 5'}
    ], U8RecLayout);

PROJECT(u8recTable, u8parseThem(LEFT));

u8recTable2 := NOFOLD(dataset([
    {U8'subtract 35 from 50'},
    {U8'divide 100 by 25 and add 5'}
    ], U8RecLayout));

PROJECT(u8recTable2, u8parseThem(LEFT));

/* Unicode multi-byte strings */
multiUstr := U'Tschüss, bis zum nächsten Mal';
REGEXFINDSET(NOFOLD(U'(\\p{L}+)|(\\p{N}+([.]\\p{N}+)?)'), multiUstr);

REGEXFINDSET(U'(\\p{L}+)|(\\p{N}+([.]\\p{N}+)?)', multiUstr);

/* UTF-8 multi-byte strings */
multiU8str := U8'Tschüss, bis zum nächsten Mal';
REGEXFINDSET(NOFOLD(U8'(\\p{L}+)|(\\p{N}+([.]\\p{N}+)?)'), multiU8str);

REGEXFINDSET(U8'(\\p{L}+)|(\\p{N}+([.]\\p{N}+)?)', multiU8str);
