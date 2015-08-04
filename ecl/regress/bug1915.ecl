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

