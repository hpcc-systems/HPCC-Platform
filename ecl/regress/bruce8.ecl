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

bigrec := record
string body{maxlength(400000)};
end;

fc := dataset('~in::samplefiles',bigrec,
csv ( separator(''), terminator('</HTML>'),maxlength(400000)));

//pattern tonexttag := pattern('.')* BEFORE '<!-';
//pattern nameofit := '[^-]'*;
//pattern tagname := '<!--' nameofit pattern('[^>]')* pattern('>') tonexttag;

//pattern tagname := '<' pattern('[^>]') before '<!--';
pattern alpha := pattern('[A-Za-z]');
pattern ws := ([' ', '\t', '\'', '-','.']); // Remove , because needed as separator for the header
pattern crlf := (['\n','\r'])+;
pattern digit := pattern('[0-9]');
//pattern usdate := repeat(digit, 1, 2) '/' repeat(digit, 1, 2)  '/' repeat(digit, 1, 4);
pattern usdate := digit{1,2} '/' digit{1,2} '/' digit{1,4};

pattern casename := (alpha | ws )+;
pattern vs := ( 'v.' | 'vs' | 'vs.');
pattern plaintiff := casename before vs;
pattern defendant := casename after vs;
pattern inre := nocase('In re');
pattern tagstart := '<!--abcfield:';
pattern tagname := alpha+ after tagstart;
pattern tagend := '-->';
pattern tag := tagstart tagname tagend;
pattern referenceid := digit+ ws* alpha ws* digit+ ws* alpha ws* digit+;
pattern courtid := '(' (ws | alpha | digit)* usdate ')';
pattern caselist := tagstart pattern('name') tagend crlf ((plaintiff vs defendant) | ( inre casename)) ',' ws* referenceid ws* courtid;
pattern contents := ANY+ before tagstart;
pattern numberpunc := (' '|'.'|','|'-');
pattern casefile := (pattern('[0-9]')|numberpunc)+;
pattern caseno := (nocase('No.') | nocase('Nos.')) casefile;
pattern namecourt := nocase('ABCDEF' pattern('[A-Z, ,0-9]')+);
pattern courtname := tagstart pattern('court') tagend ANY+ namecourt ANY+ caseno;
pattern getall := caselist ANY+ courtname;
fc1 := choosen(fc,10);

parserec := record
//  string namer := MATCHTEXT(tagname/nameofit);
    string Case_plaintiff := MATCHTEXT(plaintiff);
    string case_defendant := if (matchlength(defendant) > 0, MATCHTEXT(defendant), matchtext(casename));
//  string court := MATCHTEXT(courtname/namecourt);
//  string casenum := MATCHTEXT(courtname/caseno);
    string refid := MATCHTEXT(referenceid);
    string xcourtid := MATCHTEXT(courtid);
end;

getstuff := parse(fc1, body, caselist, parserec, first, scan);
output(getstuff);
output(fc1, { length(body), body });

