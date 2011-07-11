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

pattern dialcode := '0' pattern(U'[0-9]{2,4}');

pattern patDigit := pattern('[[:digit:]]');
pattern phonePrefix := dialcode
                    |  '(' dialcode ')'
                    ;

pattern phoneSuffix := repeat(patDigit,6,7) or repeat(patDigit,3) ' ' repeat(patDigit,3,4);
pattern phoneNumber := opt(phonePrefix ' ') phoneSuffix;

infile := dataset([
        //Should fail for these
        {'this is a 12345 number'},
        //An succeed for these
        {'this is a 123456 number'},
        {'09876 838 699'},
        {'09876 123987'},
        {'(09876) 123987'},
        {'(09876 123987)'},
        {''}
        ], { string text });


results := 
    record
        MATCHTEXT(phoneNumber);
        'Prefix:' + (string)(integer)MATCHED(phonePrefix) + '"' + MATCHTEXT(phonePrefix) + '"';
        'Suffix:' + (string)(integer)MATCHED(phoneSuffix) + '"' + MATCHTEXT(phoneSuffix) + '"';
    end;

output(PARSE(infile,text,phoneNumber,results,scan));

