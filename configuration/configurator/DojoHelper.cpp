#include "DojoHelper.hpp"
#include "SchemaElement.hpp"
#include "SchemaAnnotation.hpp"
#include "SchemaAppInfo.hpp"

bool CDojoHelper::IsElementATab(const CElement *pElement)
{
    if (pElement == NULL)
    {
        return false;
    }

    if ( (pElement->getMaxOccurs() != NULL) && (stricmp(pElement->getMaxOccurs(), "unbounded") == 0) && (isViewType(pElement, "list") == false) \
            && ( (isViewChildNodes(pElement) == true)  || (CElement::isAncestorTopElement(pElement) == true) ) )
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool CDojoHelper::isViewType(const CElement *pElement, const char* pValue)
{
    if (pElement == NULL || pValue == NULL)
    {
        return false;
    }
    else if (pElement->getAnnotation() != NULL && pElement->getAnnotation()->getAppInfo() != NULL && pElement->getAnnotation()->getAppInfo()->getViewType() != NULL)
    {
        return (stricmp(pElement->getAnnotation()->getAppInfo()->getViewType(), pValue) == 0);
    }
    else
    {
        return false;
    }
}

bool CDojoHelper::isViewChildNodes(const CElement *pElement)
{
    if (pElement == NULL)
    {
      return false;
    }
    else if (pElement->getAnnotation() != NULL && pElement->getAnnotation()->getAppInfo() != NULL && pElement->getAnnotation()->getAppInfo()->getViewChildNodes() != NULL && (stricmp(pElement->getAnnotation()->getAppInfo()->getViewChildNodes(), "false") == 0))
    {
        return false;
    }
    else
    {
        return true;
    }
}
