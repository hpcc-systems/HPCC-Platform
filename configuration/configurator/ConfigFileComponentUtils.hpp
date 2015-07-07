#ifndef _CONFIG_FILE_COMPONENT_UTILS_HPP_
#define _CONFIG_FILE_COMPONENT_UTILS_HPP_

#include "jiface.hpp"
#include "jutil.hpp"
#include "jstring.hpp"

class CConfigFileComponentUtils : public CInterface
{
public:

    IMPLEMENT_IINTERFACE

    CConfigFileComponentUtils();
    virtual ~CConfigFileComponentUtils();

    void getAvailableComponets(StringArray& compArray) const;
    void getAvailableESPServices(StringArray& compArray) const;
    void getDefinedDirectories(StringArray& definedDirectoriesArray) const;
    void getDirectoryPath(const char *pkey, StringBuffer& path) const;
    void setDirectoryPath(const char* pkey, const char* pval);

protected:

//    StringArray m_

private:

};

#endif // _CONFIG_FILE_COMPONENT_UTILS_HPP_
