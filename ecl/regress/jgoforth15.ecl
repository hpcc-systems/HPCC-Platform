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


STRING1 s1 := 'C' : stored('s1');
EBCDIC STRING sp1 := (ebcdic string)s1 : stored('sp1');
output(stringlib.data2string((data)s1));
output(stringlib.data2string((data)sp1));

STRING1 s2 := 'C' : stored('s2');
EBCDIC STRING1 sp2 := (ebcdic string)s2 : stored('sp2');
output(stringlib.data2string((data)s2));
output(stringlib.data2string((data)sp2));

STRING1 s3 := 'C';
EBCDIC STRING sp3 := (ebcdic string)s3;
output(stringlib.data2string((data)s3));
output(stringlib.data2string((data)sp3));


STRING1 s4 := 'C';
EBCDIC STRING1 sp4 := (ebcdic string1)s4;
output(stringlib.data2string((data)s4));
output(stringlib.data2string((data)sp4));

