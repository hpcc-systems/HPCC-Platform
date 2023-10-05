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

import $.setup;
import Std.File;

#onwarning(7103, ignore);

//version optRemoteRead=false
//version optRemoteRead=true

import ^ as root;
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
#option('forceRemoteRead', optRemoteRead);

prefix := setup.Files(false, false).QueryFilePrefix;
#onwarning(4523, ignore);
#onwarning(4522, ignore);
#onwarning(5406, ignore);

d1 := dataset('~file::127.0.0.1::no_such_file', {string10 f}, FLAT, OPT);
d2 := dataset(prefix + 'myeemptysuperfile', {string10 f}, FLAT, OPT);

//The following dataset would cause a long delay connnecting to a non existant dali if it was actually used
d3 := dataset('~foreign::127.0.0.1::no_such_file', {string10 f}, FLAT, OPT);

sequential(
    File.CreateSuperFile(prefix + 'myemptysuperfile'),

    output(d1);
    count(d1);
    output(d1(f='NOT'));
    count(d1(f='NOT'));

    output(d2);
    count(d2);
    output(d2(f='NOT'));
    count(d2(f='NOT'));

    File.DeleteSuperFile(prefix + 'myemptysuperfile'),
);
