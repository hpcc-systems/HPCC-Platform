#ifndef _DOJOHELPER_HPP_
#define _DOJOHELPER_HPP_

class CXSDNodeBase;

class CElement;

class CDojoHelper
{
public:

    static bool IsElementATab(const CElement *pElement);

protected:

    static bool isViewType(const CElement *pElement, const char* pValue);
    static bool isViewChildNodes(const CElement *pElement);

};

#endif // _DOJOHELPER_HPP_
