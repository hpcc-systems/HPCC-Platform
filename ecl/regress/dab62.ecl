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


r := record,maxlength(2000)
            string the_line;
  end;

d := dataset('~in::cr20041111',r,csv);

pattern f_contents := pattern('[^¦]')*;
pattern field := f_contents '¦';
pattern body := field+ f_contents;
pattern line := first body last;

r1 := record
  string l := d.the_line;
            string txt := matchtext(body/f_contents[1]);
            string txt1 := matchtext(body/f_contents[2]);
            string txt2 := matchtext(body/f_contents[3]);
            string txt3 := matchtext(body/f_contents[4]);
  end;
p := parse(d,the_line,line,r1);

output(p)



