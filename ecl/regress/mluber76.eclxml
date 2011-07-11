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
 <Module name="mal_temp">
  <Attribute name="library_impl">
   export library_impl(String1 ba=&apos;&apos;) := module,library(mal_temp.library_interface)
            export string business_code := ba;
    ENd;
  </Attribute>
  <Attribute name="library_interface">
   export library_interface(String1 ba) := INTERFACE

            export String business_code;
        
        END;
  </Attribute>
 </Module>
 <Query>
    import mal_temp;
    x:= mal_temp.library_impl;&#13;&#10;&#13;&#10;&#13;&#10;
 build(x);
 </Query>
</Archive>
