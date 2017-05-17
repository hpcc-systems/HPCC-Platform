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

MyRec := RECORD
    STRING50 Value1;
    unsigned Value2;
END;

MyRec t1(unsigned c) := transform
  SELF.value1 := 'X';
  SELF.value2 := c;
END;

ds := DATASET(100000, t1(COUNTER));

MyRec FailTransform := transform
  self.value1 := FAILMESSAGE[1..17];
  self.value2 := FAILCODE
END;

splitds := nofold(ds(Value1 != 'f'));
limited := LIMIT(splitds, 2);

recovered := CATCH(limited, SKIP);

count(splitds);
count(recovered);
