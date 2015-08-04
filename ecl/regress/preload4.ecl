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



rec := record
string123    id;
unsigned6    did;
integer6     zid1;
packed integer4 pid;
integer6     zid2;
        end;


unsigned6 didSeek := 0 : stored('didSeek');
integer4 pidSeek := 0 : stored('pidSeek');
integer6 zidSeek := 0 : stored('zidSeek');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);
filtered := rawfile(keyed(did = didSeek));
output(filtered);

filtered2 := rawfile(keyed(zid1 = zidSeek));
output(filtered2);

filtered3 := rawfile(keyed(zid2 = zidSeek));
output(filtered3);

filtered4 := rawfile(keyed(pid = pidSeek));
output(filtered4);
