/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

