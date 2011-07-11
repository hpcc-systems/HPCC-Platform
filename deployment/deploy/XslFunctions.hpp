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
