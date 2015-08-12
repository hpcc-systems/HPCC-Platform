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


testModule := MODULE

  EXPORT Original := MODULE
    EXPORT Layout := RECORD
      STRING v;
    END;

    EXPORT File := DATASET([{'a'}], Layout);
  END;


  EXPORT parent := MODULE, VIRTUAL
    SHARED Original.Layout Trans(Original.Layout L) := TRANSFORM
      SELF := L;
    END;

    EXPORT File := PROJECT(Original.File, Trans(LEFT));
  END;


  EXPORT child := MODULE(parent)
    SHARED Original.Layout Trans(Original.Layout L) := TRANSFORM
      SELF.v := L.v + '2';
    END;
  END;

END;

testmodule.child.file;
