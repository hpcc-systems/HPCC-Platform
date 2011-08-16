/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
