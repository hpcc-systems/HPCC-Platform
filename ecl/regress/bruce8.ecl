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

