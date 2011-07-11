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

  EXPORT Layout_Revenue := RECORD
  STRING30 subsidiary;         
  END;

  EXPORT File_RevenueList :=   DATASET('TEMP::RevenueList',Layout_Revenue,flat);

  Layout_Rev_Sub_BU_Prod := RECORD
   STRING30 subsidiary    := File_RevenueList.subsidiary ;       
  END;

  Layout_Rev_Sub_BU_Prod2 := RECORD
   STRING30 subsidiary;
  END;

  Rev_Sub_BU_Prod_tbl := TABLE(File_RevenueList, Layout_Rev_Sub_BU_Prod, subsidiary); 

  Layout_Rev_Sub_BU_Prod ProjectMoney(Layout_Rev_Sub_BU_Prod L) := TRANSFORM
    SELF.subsidiary := L.subsidiary;  // even explict assignment does not work
    //  SELF := L; // this does not work as well
  END;

  Rev_Sub_BU_Prod := PROJECT(Rev_Sub_BU_Prod_tbl,ProjectMoney(LEFT));

  output(Rev_Sub_BU_Prod)

