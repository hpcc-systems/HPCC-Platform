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

