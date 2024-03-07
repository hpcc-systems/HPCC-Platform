/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

//noroxie
//nohthor

import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

// Generate Data
layout_user := RECORD
   STRING20 user;
END;

ds1 := DATASET([{'Ned'},{'Robert'}, {'Jaime'}, {'Catelyn'}, {'Cersei'}, {'Daenerys'}, {'Jon'}], layout_user, DISTRIBUTED );
ds2 := DATASET([{'Sansa'}, {'Arya'}, {'Robb'}, {'Theon'}, {'Bran'}, {'Joffrey'}, {'Hound'}, {'Tyrion'}], layout_user, DISTRIBUTED);
ds3 := DATASET([{'Arya'}, {'Robb'}, {'Theon'}, {'Bran'}, {'Joffrey'}, {'Hound'}, {'Tyrion'}], layout_user, DISTRIBUTED);

dsSuperSource := DATASET(prefix+'superdata', layout_user, FLAT);
dsSuperTarget := DATASET(prefix+'super_copy', layout_user, FLAT);

checkRecords := COUNT(COMBINE(dsSuperSource, dsSuperTarget,
                      TRANSFORM(layout_user,
                              SELF.USER := IF(LEFT.USER=RIGHT.USER, LEFT.USER, ERROR('records do not match'))),
                      LOCAL, STABLE));

SEQUENTIAL(
    OUTPUT(ds1, , prefix + 'subdata1', OVERWRITE),
    OUTPUT(ds2, , prefix + 'subdata2', OVERWRITE),
    OUTPUT(ds3, , prefix + 'subdata3', OVERWRITE),
    FileServices.CreateSuperFile(prefix + 'superdata'),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(prefix + 'superdata', prefix + 'subdata1'),
    FileServices.AddSuperFile(prefix + 'superdata', prefix + 'subdata2'),
    FileServices.AddSuperFile(prefix + 'superdata', prefix + 'subdata3'),
    FileServices.FinishSuperFileTransaction(),
    FileServices.Copy(sourceLogicalName := prefix + 'superdata', destinationGroup := '', destinationLogicalName := prefix + 'super_copy', ALLOWOVERWRITE := true),
    checkRecords,
    FileServices.DeleteLogicalFile(prefix + 'super_copy', true),
    FileServices.DeleteLogicalFile(prefix + 'superdata', true),
    FileServices.DeleteLogicalFile(prefix + 'subdata1', true),
    FileServices.DeleteLogicalFile(prefix + 'subdata2', true),
    FileServices.DeleteLogicalFile(prefix + 'subdata3', true),
)
