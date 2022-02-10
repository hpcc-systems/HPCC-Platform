<Archive build="community_8.6.0-closedown0Debug"
         eclVersion="8.6.0"
         legacyImport="0"
         legacyWhen="0">
<!--

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->
 <Module key="demo" name="demo">
  <Attribute key="main"
             name="main"
             sourcePath="/home/gavin/dev/demoRepoC/demo/main.ecl"
             ts="1644325116000000">
   import layout, format;

people := DATASET([
    { &apos;gavin&apos;, &apos;changeme&apos;, 100.12345D, 10000 },
    { &apos;boris&apos;, &apos;360million&apos;, 999999.999D, -654321.123 }
], layout.person);

OUTPUT(PROJECT(people, format.personAsResult(LEFT)));&#10;
  </Attribute>
 </Module>
 <Module key="layout" name="layout">
  <Attribute key="person"
             name="person"
             sourcePath="/home/gavin/dev/demoRepoC/layout/person.ecl"
             ts="1629462352000000">
   EXPORT person := RECORD
    STRING name;
    STRING password;
    decimal30_6 balance;
    decimal30_6 savings;
END;&#10;
  </Attribute>
  <Attribute key="result"
             name="result"
             sourcePath="/home/gavin/dev/demoRepoC/layout/result.ecl"
             ts="1629129492000000">
   EXPORT result := RECORD
    STRING text;
END;&#10;
  </Attribute>
 </Module>
 <Module key="format" name="format">
  <Attribute key="personasresult"
             name="personAsResult"
             sourcePath="/home/gavin/dev/demoRepoC/format/personAsResult.ecl"
             ts="1644325120000000">
   IMPORT layout;
import demoRepoD as demoD;

EXPORT layout.result personAsResult(layout.person input) := TRANSFORM
    SELF.text := $.personAsText2(input);
END;&#10;
  </Attribute>
  <Attribute key="personastext2"
             name="personAsText2"
             sourcePath="/home/gavin/dev/demoRepoC/format/personAsText2.ecl"
             ts="1644407176000000">
   IMPORT layout;
import demoRepoD as demoD;

EXPORT personAsText2(layout.person input) := DemoD.formatHelper.formatPersonNested.doFormat(input);&#10;
  </Attribute>
 </Module>
 <Module key="demorepod" name="demoRepoD" package="git+ssh://git@github.com/ghalliday/gch-ecldemo-d.git#644c4585221f4dd80ca1e8f05974983455a244e5"/>
 <Archive package="git+ssh://git@github.com/ghalliday/gch-ecldemo-d.git#644c4585221f4dd80ca1e8f05974983455a244e5">
  <Module key="formathelper" name="formatHelper">
   <Attribute key="formatpersonnested"
              name="formatPersonNested"
              sourcePath="/home/gavin/dev/demoRepoD/formatHelper/formatPersonNested.ecl"
              ts="1644406999000000">
    &#10;EXPORT formatPersonNested := MODULE
    EXPORT doFormat(input) := FUNCTIONMACRO
        import #$.^.format as formatter;
        RETURN input.name + &apos;: &apos; + formatter.maskPassword(input.password)
                    + &apos; {&apos; + (string)formatter.formatMoney(input.balance)
                    + &apos; / &apos; + (string)formatter.formatMoney(input.savings) + &apos;}&apos;;

    ENDMACRO;
END;&#10;
   </Attribute>
  </Module>
  <Module key="format" name="format">
   <Attribute key="maskpassword"
              name="maskPassword"
              sourcePath="/home/gavin/dev/demoRepoD/format/maskPassword.ecl"
              ts="1629128893000000">
    import Std.Str;
export STRING maskPassword(STRING value) := IF (LENGTH(value) &gt; 2, value[1] + Str.Repeat(&apos;*&apos;, LENGTH(value)-2) + value[LENGTH(value)], Str.Repeat(&apos;*&apos;, LENGTH(value)));&#10;
   </Attribute>
   <Attribute key="formatmoney"
              name="formatMoney"
              sourcePath="/home/gavin/dev/demoRepoD/format/formatMoney.ecl"
              ts="1629128893000000">
    export utf8 formatMoney(decimal30_6 value) := &apos;$&apos; + (utf8)(decimal26_2)value;&#10;
   </Attribute>
  </Module>
  <Module key="std" name="std"/>
 </Archive>
 <Option name="debugquery" value="1"/>
 <Option name="savecpptempfiles" value="1"/>
 <Option name="spanmultiplecpp" value="0"/>
 <Query attributePath="demo.main"/>
</Archive>
