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

layout_u6 := RECORD
  unsigned6 dids;
END;

layout_1 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
END;
OUTPUT (sizeof (layout_1), NAMED ('sizeof_1'));

layout_2 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(1)};
END;
OUTPUT (sizeof (layout_2, MAX), NAMED ('sizeof_2'));

layout_3 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(1)};
  DATASET (layout_u6) id  {MAXCOUNT(1)};
END;
OUTPUT (sizeof (layout_3, MAX), NAMED ('sizeof_3'));

layout_4 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(2)};
END;
OUTPUT (sizeof (layout_4, MAX), NAMED ('sizeof_4'));

layout_5 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(2)};
  DATASET (layout_u6) id  {MAXCOUNT(2)};
END;
OUTPUT (sizeof (layout_5, MAX), NAMED ('sizeof_5'));
