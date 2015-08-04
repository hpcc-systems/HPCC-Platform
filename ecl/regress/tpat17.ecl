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


r := record
  string line;
  end;

d := dataset(['one','two','THREE','four','Gavin','Hawthorn','123AGE','WHEN123']
,r);


token x := pattern('[a-zA-Z]+');
rule xupper := x in pattern('[A-Z]+');
rule xlower := x in pattern('[a-z]+');
rule xgavin := validate(x, matchtext = 'Gavin');
rule xhalliday := validate(x, matchtext = 'Hawthorn');
rule x4     := x length(4);
rule x5_7   := x length(5..7);
rule xfirst := first x;
rule xlast  := x last;
rule xbefore := x before pattern('[0-9]+');
rule xafter := x after pattern('[0-9]+');


rule s := xupper | xlower | xgavin | xhalliday | x4 | x5_7 | xfirst | xlast | xbefore | xafter;

results :=
    record
        MATCHTEXT;
        matched(xupper);
        matched(xlower);
        matched(xgavin);
        matched(xhalliday);
        matched(x4);
        matched(x5_7);
        matched(xfirst);
        matched(xlast);
        matched(xbefore);
        matched(xafter);
    end;

output(PARSE(d,line,s,results));
//output(PARSE(d,line,s,results,parse));
