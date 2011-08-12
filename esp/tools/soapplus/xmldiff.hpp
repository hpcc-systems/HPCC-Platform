/*##############################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

#ifndef __XMLDIFF_HPP__
#define __XMLDIFF_HPP__
#include "jliball.hpp"
#undef new
#include <set>
#include <string>
#include <map>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

class CXmlDiff : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    
    CXmlDiff(IProperties* globals, const char* left, const char* right, IPropertyTree* cfgtree);
    virtual ~CXmlDiff() {}
    bool compare();

private:
    StringBuffer m_left, m_right;
    bool m_filenamesPrinted;
    std::set<std::string> m_ignoredXPaths;
    IPropertyTree* m_cfgtree;
    std::map<std::string, int> m_diffcountcache;
    std::map<std::string, bool> m_compcache;
    int m_difflimit;
    bool m_ooo;
    IProperties* m_globals;

    void printPtree(const char* prefix, const char* xpath, IPropertyTree* t, const char* xpathFull);
    bool diffPtree(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull);
    bool cmpPtree(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull);
    int countDiff(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull);
    int countLeaves(IPropertyTree* t);
    void printDiff(const char *fmt, ...) __attribute__((format(printf, 2, 3)));
    bool cmpAttributes(IPTree* t1, IPTree* t2, const char* xpath, bool print);
    void getAttrString(IPropertyTree* t1, StringBuffer& attrBuff);
};

#endif
