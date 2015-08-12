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

rec := RECORD
    STRING10 field1;
    STRING60 field2;
    STRING14 field3;
    STRING30 field4;
END;

combinedRec := RECORD
    STRING longLine;
END;

ds := DATASET ([{'ROW1FLD1','ROW1FLD2','ROW1FLD3','ROW1FLD4'},
                {'ROW2FLD1','ROW2FLD2','ROW2FLD3','ROW2FLD4'}], rec);

combinedRec fieldToStringXform (rec L) := TRANSFORM
    SELF.longLine := transfer(L, STRING114);
END;

combinedDS := PROJECT (ds, fieldToStringXform (LEFT));

OUTPUT(combinedDs);
