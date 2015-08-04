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

#option ('globalFold', 1);

r := record
  string1000000 line1;
  varstring1000000 line2;
  unicode1000000 line3;
  varunicode1000000 line4;
  qstring1000000 line5;
  data1000000 line6;
  end;

ds := dataset([
{(string1000000)'a',(varstring1000000)'b',(unicode1000000)'c',(varunicode1000000)'d',(qstring1000000)'e',(data1000000)'f'}],r);


output(ds);


r2 := record
  string1000 line1;
  varstring1000 line2;
  unicode1000 line3;
  varunicode1000 line4;
  qstring1000 line5;
  data1000 line6;
  end;

ds2 := dataset([
{(string1000)'a',(varstring1000)'b',(unicode1000)'c',(varunicode1000)'d',(qstring1000)'e',(data1000)'f'}],r2);


output(ds2);

r3 := record
  string1000 line1;
  string1000 line2;
  unicode1000 line3;
  unicode1000 line4;
  string1000 line5;
  string1000 line6;
  end;

ds3 := dataset([
{(string1000)'a',(varstring1000)'b',(unicode1000)'c',(varunicode1000)'d',(qstring1000)'e',(data1000)'f'}],r3);


output(ds3);
