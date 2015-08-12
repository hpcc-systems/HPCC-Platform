<Archive useArchivePlugins="1">
    <!--

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
