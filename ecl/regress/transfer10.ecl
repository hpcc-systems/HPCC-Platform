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

EXPORT RI01_Rec := RECORD
    STRING3     UnitNum;
    STRING4  RecCode;

    STRING6  AcctNum;
    STRING3     AcctSufNum;
END;

DS := DATASET([{'001','Rec1','Acc001','001'}], RI01_Rec);

combinedRec := record
string16 combined;
    end;
    
p := project(ds, transform(combinedRec, SELF.combined := transfer(LEFT, string16)));
output(p);
