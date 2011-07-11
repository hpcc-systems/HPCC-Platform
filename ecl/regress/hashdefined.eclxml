<Archive useArchivePlugins="1">
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
    <Module name="common">
    <Attribute name="codesDataset">
      export codesDataset := dataset([], { integer code });
    </Attribute>
    <Attribute name="idsDataset">
      export idsDataset := dataset([], { integer code });
    </Attribute>
  </Module>
  <Module name="override">
    <Attribute name="atOpen">
      export atOpen() := macro
      output('hackaround atOpen bug')
      endmacro;
    </Attribute>
    <Attribute name="afterClose">
      export afterClose() := macro
      output('clean up messy stuff')
      endmacro;
    </Attribute>
    <Attribute name="salt">
export salt := MODULE
    export codesDataset := dataset([100,20], { integer code });
end;
    </Attribute>
    <Attribute name="sugar">
     export sugar(integer c1) := MODULE
      export codesDataset := dataset([c1], { integer code });
      end;
    </Attribute>
  </Module>
  <Query>
     import common,override;

    #IFDEFINED(override.beforeOpen());
    output('beforeOpen');

    #if (#ISDEFINED(override.atOpen))
    override.atOpen();
    #end

    output('doSomething');

    #if (#ISDEFINED(override.atClose))
    override.atClose();
    #end
    output('afterClose');
    #IFDEFINED(override.afterClose());

    getDataset(ds, overridename) := macro
    #if (#ISDEFINED(#EXPAND(overridename)))
    #EXPAND(overridename)
    #else
    ds
    #end
    endmacro;

    #IF (#ISDEFINED(override.salt.codesDataset))
    ds := override.salt.codesDataset;
    #ELSE
    ds := common.codesDataset;
    #end
    output(ds,,'inline');
    output(getDataset(common.codesDataset, 'override.salt.codesDataset'),,'via macro');
    output(#IFDEFINED(override.salt.codesDataset, common.codesDataset),,'via #defined macro');
    output(#IFDEFINED(override.sugar(99).codesDataset, common.codesDataset),,'via #defined functional macro');
    output(getDataset(common.idsDataset, 'override.salt.idsDataset'),,'not overriden via macro');

  </Query>
</Archive>
