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

I := record
 unsigned integer1 presflag2;
 ifblock(self.presflag2 & 0x08 != 0)
    decimal3 mem_num1;
 end;

end;

tempdataset := dataset('ecl_test::fb38_sample', I, flat);
O := record
 unsigned integer1 presflag2;

 decimal3 mem_num1;
end;

O trans(i l) := transform
  self := l;
  end;

p := project(tempdataset,trans(left));

output(p)