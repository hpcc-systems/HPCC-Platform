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

import lib_fileservices;

rec := RECORD
      STRING10 S;
       END;

srcnode := '10.150.199.2';
srcdir  := '/c$/test/';

dir := FileServices.RemoteDirectory(srcnode,srcdir,'*.txt',true);

SEQUENTIAL(
FileServices.DeleteSuperFile('MultiSuper1'),
FileServices.CreateSuperFile('MultiSuper1'),
FileServices.StartSuperFileTransaction(),
apply(dir,FileServices.AddSuperFile('MultiSuper1',name,,true)),
FileServices.FinishSuperFileTransaction()
);
