/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    MapStringTo<bool> m_compcache;
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
