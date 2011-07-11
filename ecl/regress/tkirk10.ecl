/*##############################################################################

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
############################################################################## */

/*
Gavin-

I'm wondering if there is still a somewhat obscure issue with the xml(heading()) taking a non-constant
parameter.  It's also possible we're just trying to do something that shouldn't be allowed.

In the xml(heading()) section below, including the pHeadingFromFunction parameter will cause the error
"eclserver 1: Unexpected operator in: HqlCppTranslator::buildExpr(pPassingThruFunction; ) : operator = <parameter>"
to be generated and the job does not run.  Commenting out that line and using the one below it
instead will allow the job to run.

This problem only appears to happen when passing the parameter to a function that then invokes a
macro that includes that parameter as a part of the xml(heading()) string.

Let me know if I can help or elaborate on any of it.  We have worked around this by using macros
instead of functions, but it's one of those that syntax checks okay but fails when launched.

Thanks!
-Tony
*/
//--------------------------------------------------------------------------------------------------
dInline :=  dataset([{'One Record','Two Fields'}],{string Field1, string Field2});
//--------------------------------------------------------------------------------------------------
mTestMacro(pDataset, pHeadingDirect, pHeadingFromFunction, zOutReference)
 :=
  macro
    zOutReference :=
        output(pDataset,,'temp::tkirk::delete_me_' + workunit + '.xml',
                xml(heading('<Dataset ' + 'pHeadingDirect="' + pHeadingDirect + '" '
//--------------------------------------------------------------------------------------------------
// Select only ONE of the following two lines.  The first one will cause an error, the next one will not.
                          + 'pHeadingThruFunction="' + pHeadingFromFunction + '" '
//                          + 'pHeadingThruFunction="" '
//--------------------------------------------------------------------------------------------------
                          + '>\n',
                            '</Dataset>\n'
                           ),trim
                   ),overwrite
               );
  endmacro
 ;
//--------------------------------------------------------------------------------------------------
fFunction(string pPassingThruFunction)
 :=
  function
    mTestMacro(dInLine,'Direct_Heading',pPassingThruFunction,zDoThis);
    return zDoThis;
  end
 ;
//--------------------------------------------------------------------------------------------------

fFunction('20061007');

//--------------------------------------------------------------------------------------------------
