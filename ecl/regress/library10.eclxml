<!--

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
-->
<Archive useArchivePlugins="1">
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



