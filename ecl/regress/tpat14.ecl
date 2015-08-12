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
{'abc * def + ghi  '},
{'(a + b) * (c + d)'},
{''}],r);


//This doesn't run, it just checks syntax...

pattern p1 := any+?;
pattern p2 := PATTERN('[^[]');
pattern s := p1 p2;

results :=
    record
        MATCHTEXT;
    end;

outfile1 := PARSE(d,line,s,results,first,whole);
output(outfile1);
