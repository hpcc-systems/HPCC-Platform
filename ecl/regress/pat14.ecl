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

infile := dataset([
        {'RTC AS RECEIVERCONSERVATOR OF CARTERET SAVINGS BANK FA SUCCESSOR IN INTEREST TO OR FORMERLY KNOWN AS WESTERN FEDERA'},
        {''}
        ], { string line });

PATTERN ws := ' ';
PATTERN word_as := ws 'AS' ws;
PATTERN name := min(ANY+);
PATTERN before_as := name word_as;
pattern sentance := before_as ;


results :=
    record
//      MATCHTEXT;
        MATCHTEXT(before_as);
    end;

//Return first matching sentance that we can find
output(PARSE(infile,line,sentance,results));
