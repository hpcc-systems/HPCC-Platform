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
#if 0 //this is just a sample Xalan (XSLT) external function
#include "XslFunctions.hpp"
#include "XPath/XObjectFactory.hpp"



FunctionSample::FunctionSample()
{
}



FunctionSample::~FunctionSample()
{
}



XObjectPtr
FunctionSample::execute(
            XPathExecutionContext&  executionContext,
            XalanNode*              /* context */,          
            const XObjectPtr        arg1,
            const XObjectPtr        arg2,
            const Locator*          /* locator */) const
{
   assert(arg1.null() == false);
   assert(arg2.null() == false);

   XalanDOMString path;
   arg1->str(path);
   const bool bLinux = arg2->boolean();

   XalanDOMChar dchOld;
   XalanDOMChar dchNew;

   if (bLinux)
   {
      dchOld = '\\';
      dchNew = '/';
   }
   else
   {
      dchOld = '/';
      dchOld = '\\';
   }

    int len = path.length();
   for (int i=0; i<len; i++)
      if (path[i] == dchOld)
         path[i] = dchNew;

    return executionContext.getXObjectFactory().createString(path); 
}



#if defined(XALAN_NO_COVARIANT_RETURN_TYPE)
Function*
#else
FunctionSample*
#endif
FunctionSample::clone() const
{
    return new FunctionSample(*this);
}



const XalanDOMString
FunctionSample::getError() const
{
    return StaticStringToDOMString(XALAN_STATIC_UCODE_STRING("The boolean() function takes one argument!"));
}
#endif //0

