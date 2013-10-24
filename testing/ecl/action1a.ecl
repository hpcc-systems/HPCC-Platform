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

r := { string text; };

msg(string x) := DATASET([x], {r});

o1 := OUTPUT(msg('One'),,NAMED('msgs'),EXTEND);
o2 := OUTPUT(msg('Two'),,NAMED('msgs'),EXTEND);
o3 := OUTPUT(msg('Three'),,NAMED('msgs'),EXTEND);

msgs := DATASET(WORKUNIT('msgs'), r);

//Should have the same values for the two output(counts), and should be 3
t1 := ORDERED(o1,o2,o3,OUTPUT(count(msgs)),o3,o2,o1,OUTPUT(count(msgs)));

t2 := ORDERED(o3,o2,o1,OUTPUT(count(msgs)),o1,o2,o3,OUTPUT(count(msgs)));

boolean updown := true : stored('updown');

IF(updown, t1, t2);
