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
#if !defined(__XSL_FUNCTIONS_HPP__)
#define __XSL_FUNCTIONS_HPP__

#if 0 //this is just a sample Xalan (XSLT) external function

#include "deploy.hpp"

// Base header file.  Must be first.
#include "XPath/XPathDefinitions.hpp"

// Base class header file...
#include "XPath/Function.hpp"



/**
* XPath implementation of a function.that either replaces
* all \\ by / on linux systems or viceversa for windows
*/
class DEPLOY_API FunctionSample : public Function
{
public:
    
    FunctionSample();
    
    virtual ~FunctionSample();
    
    // These methods are inherited from Function ...
    
    virtual XObjectPtr execute(
        XPathExecutionContext&  executionContext,
        XalanNode*              context,            
        const XObjectPtr        arg1,
        const XObjectPtr        arg2,
        const Locator*          locator) const;
    
#if defined(XALAN_NO_COVARIANT_RETURN_TYPE)
    virtual Function*   clone() const;
#else
    virtual FunctionSample* clone() const;
#endif
    
protected:
    const XalanDOMString    getError() const;
    
private:
    // Not implemented...
    Function& operator=(const FunctionSample&);
    bool operator==(const FunctionSample&) const;
};


#endif //0
#endif // __XSL_FUNCTIONS_HPP__
