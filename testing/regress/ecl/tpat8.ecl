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


r := record
  string line;
  end;

ds := dataset([
{'baaaaaaaabaaaabaaaaaaabaaaaabaaaaaab'},
{''}],r);


pattern S := 'b' 'a'* 'b';

results := 
    record
        string x := MATCHTEXT;
    end;

output(PARSE(ds,line,S,results,scan all,all));
output(PARSE(ds,line,S,results,maxlength(7),scan all,all));
