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
