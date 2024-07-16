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

import Std.System;
import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

// Generate Data
layout_user := RECORD
   STRING20 user;
END;

layout_names := RECORD
   STRING30 name;
END;

users1 := DATASET([{'Ned'},{'Robert'}, {'Jaime'}, {'Catelyn'}, {'Cersei'}, {'Daenerys'}, {'Jon'}], layout_user );
users2 := DATASET([{'Sansa'}, {'Arya'}, {'Robb'}, {'Theon'}, {'Bran'}, {'Joffrey'}, {'Hound'}, {'Tyrion'}], layout_user );
users3 := DATASET([{'Arya'}, {'Robb'}, {'Theon'}, {'Bran'}, {'Joffrey'}, {'Hound'}, {'Tyrion'}], layout_user );
                   
names := DATASET([{'Jack'},{'Oliver'},{'James'},{'Charlie'},{'Harris'},{'Lewis'},{'Leo'},{'Noah'},{'Alfie'},{'Rory'},{'Alexander'},{'Max'},{'Logan'},
                  {'Lucas'},{'Harry'},{'Theo'},{'Thomas'},{'Brodie'},{'Archie'},{'Jacob'},{'Finlay'},{'Finn'},{'Daniel'},{'Joshua'},{'Oscar'},{'Arthur'},
                  {'Hunter'},{'Ethan'},{'Mason'},{'Olivia'},{'Emily'},{'Isla'},{'Sophie'},{'Ella'},{'Ava'},{'Amelia'},{'Grace'},{'Freya'},{'Charlotte'},
                  {'Jessica'},{'Lucy'},{'Ellie'},{'Sophia'},{'Aria'},{'Lily'},{'Harper'},{'Mia'},{'Rosie'},{'Millie'},{'Evie'},{'Eilidh'},{'Ruby'},
                  {'Willow'},{'Anna'},{'Maisie'},{'Hannah'},{'Eva'},{'Chloe'}], layout_names);
layout_rec := RECORD
  STRING20 User;
  STRING20 FirstName;
  INTEGER id; 
END;

layout_rec f(layout_user l, layout_names r, INTEGER c) := TRANSFORM
  SELF.user := TRIM(l.user,right)  + (c % 10000);
  SELF.FirstName := r.name;
  SELF.id := c;
END;

v1 := JOIN(users1, names, true, f(LEFT,RIGHT,COUNTER), ALL);
v2 := JOIN(users2, names, true, f(LEFT,RIGHT,COUNTER), ALL);
v3 := JOIN(users3, names, true, f(LEFT,RIGHT,COUNTER), ALL);

layout_rec addCount(layout_rec l, INTEGER c) := TRANSFORM
  SELF.id :=  c;
  SELF := L;
END;

d1 := DISTRIBUTE(NORMALIZE(v1, 1000, addCount(LEFT, COUNTER)), HASH32(id));
d2 := DISTRIBUTE(NORMALIZE(v2, 1000, addCount(LEFT, COUNTER)), HASH32(id));
d3 := DISTRIBUTE(NORMALIZE(v3, 1000, addCount(LEFT, COUNTER)), HASH32(id));

SEQUENTIAL(
    OUTPUT(d1, , prefix + 'subdata1', OVERWRITE),
    OUTPUT(d2, , prefix + 'subdata2', OVERWRITE),
    OUTPUT(d3, , prefix + 'subdata3', OVERWRITE),
    FileServices.CreateSuperFile(prefix + 'superdata'),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(prefix + 'superdata',prefix + 'subdata1'),
    FileServices.AddSuperFile(prefix + 'superdata',prefix + 'subdata2'),
    FileServices.AddSuperFile(prefix + 'superdata',prefix + 'subdata3'),
    FileServices.FinishSuperFileTransaction(),
    FileServices.Copy(sourceLogicalName := prefix + 'superdata', destinationGroup:= 'mythor', destinationLogicalName := prefix + 'super_copy', ALLOWOVERWRITE := true),
    FileServices.DeleteLogicalFile(prefix+'super_copy', true),
    FileServices.DeleteOwnedSubFiles(prefix + 'superdata'),
    FileServices.DeleteLogicalFile(prefix+'subdata1', true),
    FileServices.DeleteLogicalFile(prefix+'subdata2', true),
    FileServices.DeleteLogicalFile(prefix+'subdata3', true),
    FileServices.DeleteLogicalFile(prefix+'superdata', true),
)

