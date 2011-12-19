#include "jexcept.hpp"
#include "xslprocessor.hpp"
#include "xmlvalidator.hpp"
#include "xmlerror.hpp"

XMLLIB_API IXmlDomParser* getXmlDomParser()
{
    throw MakeStringException(XMLERR_MissingDependency, "XML validation library unavailable");
}

extern IXslProcessor* getXslProcessor()
{
    throw MakeStringException(XMLERR_MissingDependency, "XSLT library unavailable");
}
