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

// this may cause memory leak
//  dd := 3;

  s1 := service
    integer dd() : entrypoint='dd';   //* redefiniton error although it should be ok. <== BUG
    integer ddx() : entrypoint='ddx';
    integer dd() : entrypoint='dd';
  end;

  s2 := service
    integer dd() : entrypoint='dd';   //* redefiniton error although it should be ok. <== BUG
    integer ddx() : entrypoint='ddx';  //* OK although it is already defined in s1
  end;

  ddx := 3; //* OK although it is already defined in s1 and s2

  s1.dd() + s2.dd();

  r := record
     integer1 dd;
     integer1 dd;
     integer1 ddx;
     integer1 r_f;
  end;

  r2 := record
     integer1 dd;
     integer1 ddx;
  end;

  r_f := 3;
