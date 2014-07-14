<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
<Archive useArchivePlugins="1" legacyImport="1">
 <Module name="example">
  <Attribute name="namesRecord">

export namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;
  </Attribute>
  <Attribute name="oldAge" flags="65536">
  export oldAge := 65;
  </Attribute>
  <Attribute name="FilterLibrary" flags="65536">
export filterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;
  </Attribute>
  <Attribute name="FilterDataset" flags="65536">
export filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := library('NameFilter', FilterLibrary(ds, search, onlyOldies));
  </Attribute>
 </Module>
 <Query>
    import example;
#option ('noCache', true);
namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], example.namesRecord);

filtered := example.filterDataset(namesTable, 'Smith', false);
output(filtered.included,,named('Included'));

filtered2 := example.filterDataset(namesTable, 'Hawthorn', false);
output(filtered2.excluded,,named('Excluded'));
 </Query>
</Archive>



