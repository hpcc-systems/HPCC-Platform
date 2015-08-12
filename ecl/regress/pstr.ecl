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


pstring := type
  export integer physicallength(string x) := transfer(x[1],unsigned1)+1;
  export string load(string x) := x[2..transfer(x[1],unsigned1)+1];
  export string store(string x) := transfer(length(x),string1)+x;
  end;

r := record
     pstring a;
     end;

//d := dataset('in.d00', r,FLAT);
d := dataset([{7},{83},{85},{67},{67},{69},{83},{83}],r);
//d := dataset([{7}],r);

r1 := record
  string100 a1 := d.a;
      end;

output(nofold(d),r1);


