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

testStringlib := MODULE
  EXPORT AaaStartup := OUTPUT('Begin test');

  EXPORT Test0 := 'This should not be output by evaluate';

  EXPORT TestConstant := MODULE
    EXPORT Test1 := dataset([1],{integer x});
    EXPORT Test2 := dataset([2,3],{integer xx});
  END;

  EXPORT TestOther := MODULE
    EXPORT Test1 := dataset([99],{integer xy});
    EXPORT Test2 := dataset([98,87],{integer xz});
  END;

  EXPORT ZzzClosedown := OUTPUT('End test');
END;

testAllPlugins := MODULE
  EXPORT testStringLib := testStringLib;
END;

OUTPUT(testAllPlugins);
