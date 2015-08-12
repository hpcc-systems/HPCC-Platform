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

d := dataset([
{'AB12345678901234567BA'}
],r);


pattern s := 'AB' any* 'BA';

results :=
    record
        MATCHTEXT;
    end;

//Should output nothing...
outfile1 := PARSE(d,line,s,results,maxlength(20));
output(outfile1);

//Should match.
outfile2 := PARSE(d,line,s,results,maxlength(21));
output(outfile2);

