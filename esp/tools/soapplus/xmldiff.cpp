/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

#pragma warning(disable:4786)

#include "xmldiff.hpp"
#include <string>
#include <set>
#include <vector>

#include "jliball.hpp"

CXmlDiff::CXmlDiff(IProperties* globals, const char* left, const char* right, IPropertyTree* cfgtree)
{
    m_globals = globals;
    m_left.append(left);
    m_right.append(right);
    m_filenamesPrinted = false;

    m_cfgtree = cfgtree;
    
    if(globals && globals->hasProp("difflimit"))
        m_difflimit = globals->getPropInt("difflimit");
    else
        m_difflimit = 0;

    if(globals && globals->hasProp("ooo"))
        m_ooo = true;
    else
        m_ooo = false;

    if(cfgtree)
    {
        Owned<IPropertyTreeIterator> xpaths = m_cfgtree->getElements("Ignore/XPath");
        for (xpaths->first(); xpaths->isValid(); xpaths->next())
        {
            IPropertyTree& xpath = xpaths->query();
            StringBuffer xpathbuf(xpath.queryProp("."));
            if(xpathbuf.charAt(xpathbuf.length() - 1) == '/')
            {
                xpathbuf.setCharAt(xpathbuf.length() - 1, '\0');
            }
            m_ignoredXPaths.insert(xpathbuf.str());
        }
    }
}
    
bool CXmlDiff::compare()
{
    const char* left = m_left.str();
    const char* right = m_right.str();

    m_filenamesPrinted = false;

    if(!left || !right || !*left || !*right)
        return false;

    Owned<IFile> lf = createIFile(left);
    if(!lf->exists())
    {
        printDiff("%s doesn't exist\n", left);
        return false;
    }

    Owned<IFile> rf = createIFile(right);
    if(!rf->exists())
    {
        printDiff("%s doesn't exist\n", right);
        return false;
    }

    if(lf->isDirectory() && !rf->isDirectory())
    {
        printDiff("%s is a directory, while %s is not\n", left, right);
        return false;
    }
    else if(!lf->isDirectory() && rf->isDirectory())
    {
        printDiff("%s is a directory, while %s is not\n", right, left);
        return false;
    }
    else if(lf->isDirectory() && rf->isDirectory())
    {
        Owned<IDirectoryIterator> ldi = lf->directoryFiles(NULL, false, true);
        Owned<IDirectoryIterator> rdi = rf->directoryFiles(NULL, false, true);

        if(ldi.get() && rdi.get())
        {
            using namespace std;
            StringArray leftnames;
            StringArray rightnames;
            StringArray commonnames;
            set<string> leftset;
            set<string> rightset;
            ForEach(*ldi)
            {
                IFile &file = ldi->query();
                StringBuffer fname(file.queryFilename());
                getFileNameOnly(fname, false);
                leftnames.append(fname.str());
                leftset.insert(fname.str());
            }
            ForEach(*rdi)
            {
                IFile &file = rdi->query();
                StringBuffer fname(file.queryFilename());
                getFileNameOnly(fname, false);
                rightnames.append(fname.str());
                rightset.insert(fname.str());
            }
            ForEachItemIn(i, leftnames)
            {
                const char* fname = leftnames.item(i);
                if(!fname || !*fname)
                    continue;
                if(rightset.find(fname) != rightset.end())
                {
                    commonnames.append(fname);
                }
                else
                {
                    printDiff("< %s\n", fname);
                }
            }
            ForEachItemIn(j, rightnames)
            {
                const char* fname = rightnames.item(j);
                if(!fname || !*fname)
                    continue;
                if(leftset.find(fname) == leftset.end())
                {
                    printDiff("> %s\n", fname);
                }
            }
            ForEachItemIn(k, commonnames)
            {
                StringBuffer lpath(left);
                StringBuffer rpath(right);
                lpath.append(PATHSEPSTR).append(commonnames.item(k));
                rpath.append(PATHSEPSTR).append(commonnames.item(k));

                try
                {
                    Owned<CXmlDiff> xmldiff = new CXmlDiff(m_globals, lpath.str(), rpath.str(), m_cfgtree);
                    xmldiff->compare();
                }
                catch(IException* e)
                {
                    StringBuffer errmsg;
                    e->errorMessage(errmsg);
                    printDiff("%s", errmsg.str());
                    e->Release();
                }
            }
        }
    }
    else
    {
        Owned<IPropertyTree> ltree;
        try
        {
            ltree.setown(createPTreeFromXMLFile(left));
        }
        catch(...)
        {
            try
            {
                StringBuffer leftxmlbuf;
                leftxmlbuf.loadFile(left);
                const char* leftxml = leftxmlbuf.str();
                if(leftxml)
                    leftxml = strchr(leftxml, '<');

                if(leftxml)
                {
                    ltree.setown(createPTreeFromXMLString(leftxml));
                }
            }
            catch(...)
            {
            }
        }

        Owned<IPropertyTree> rtree;

        try
        {
            rtree.setown(createPTreeFromXMLFile(right));
        }
        catch(...)
        {
            try
            {
                StringBuffer rightxmlbuf;
                rightxmlbuf.loadFile(right);
                const char* rightxml = rightxmlbuf.str();
                if(rightxml)
                    rightxml = strchr(rightxml, '<');

                if(rightxml)
                {
                    rtree.setown(createPTreeFromXMLString(rightxml));
                }
            }
            catch(...)
            {
            }
        }

        if(!ltree.get() || !rtree.get())
        {
            if(!ltree.get())
            {
                printDiff("< can't parse %s\n", left);
            }

            if(!rtree.get())
            {
                printDiff("> can't parse %s\n", right);
            }
            
            return false;
        }

        //TimeSection ts("diffPtree");
        try
        {
            diffPtree("", ltree.get(), rtree.get(), "");
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            printDiff("%s", errmsg.str());
            e->Release();
        }
    }

    return true;
}

void CXmlDiff::getAttrString(IPropertyTree* t1, StringBuffer& attrBuf)
{
    Owned<IAttributeIterator> t1Iter = t1->getAttributes();
    attrBuf.clear();

    t1Iter->first();
    while (t1Iter->isValid())
    {
        attrBuf.appendf("%s='%s'", t1Iter->queryName(), t1Iter->queryValue());
        t1Iter->next();

        if (t1Iter->isValid())
            attrBuf.appendf(" ");
    }
}

bool CXmlDiff::cmpAttributes(IPTree* t1, IPTree* t2, const char* xpathFull, bool print)
{
    Owned<IAttributeIterator> a1 = t1->getAttributes(true);
    Owned<IAttributeIterator> a2 = t2->getAttributes(true);
    bool diff = false;

    for (a1->first(), a2->first(); a1->isValid() && a2->isValid(); a1->next(),a2->next())
    {
        if (strcmp(a1->queryName(),a2->queryName()) || 
                strcmp(a1->queryValue(),a2->queryValue()))
        {
            diff = true;
            break;
        }
    }

    if (a1->isValid() || a2->isValid())
        diff = true;

    if (diff && print && xpathFull)
    {
        StringBuffer xpathFullBuf(xpathFull);
        const char* r = strrchr(xpathFull, ']');
        const char* slash = strrchr(xpathFull, '/');
        if(r > slash) //Only remove the attributes in the last tag
        {
            const char* l = strrchr(xpathFull, '[');
            if(l > slash && l < r)
            {
                xpathFullBuf.remove(l - xpathFull, r - l + 1);
            }
        }
        StringBuffer attrString;
        getAttrString(t1, attrString);
        printDiff("< %s[%s]\n", xpathFullBuf.str(), attrString.str());
        getAttrString(t2, attrString);
        printDiff("> %s[%s]\n", xpathFullBuf.str(), attrString.str());
    }

    return diff;
}

bool CXmlDiff::cmpPtree(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull)
{
    if(t1 == t2)
        return true;

    if(!t1 || !t2)
    {
        if(!t1)
        {
            if(t2)
                return false;
            else
                return true;
        }
        else
            return false;
    }

    StringBuffer keybuf;
    keybuf.appendf("%p-%p", t1, t2);
    std::string key = keybuf.str();
    if(m_compcache.getValue(key.c_str()) != NULL)
    {
        return *(m_compcache.getValue(key.c_str()));
    }


    if(xpath && *xpath)
    {
        if(m_ignoredXPaths.find(xpath) != m_ignoredXPaths.end())
        {
            m_compcache.setValue(key.c_str(),true);
            return true;
        }
    }

    const char* name1 = t1->queryName();
    const char* name2 = t2->queryName();
    if((name1 && !name2) || (!name1 && name2) || (name1 && name2 && strcmp(name1, name2) != 0))
    {
        m_compcache.setValue(key.c_str(),false);
        return false;
    }

    // compare attributes
    if (cmpAttributes(t1,t2,xpathFull,false))
    {
        m_compcache.setValue(key.c_str(),false);
        return false;
    }

    if(t1->hasChildren() && t2->hasChildren())
    {
        int num1 = t1->numChildren();
        int num2 = t2->numChildren();
        if(num1 != num2)
        {
            m_compcache.setValue(key.c_str(),false);
            return false;
        }

        IPropertyTree** ptrs1 = new IPropertyTree*[num1];
        IPropertyTree** ptrs2 = new IPropertyTree*[num2];
        int i;
        for(i = 0; i < num1; i++)
            ptrs1[i] = NULL;
        for(i = 0; i < num2; i++)
            ptrs2[i] = NULL;

        Owned<IPropertyTreeIterator> mi1 = t1->getElements("*");
        if(mi1.get())
        {
            i = 0;
            for(mi1->first(); mi1->isValid(); mi1->next())
            {
                ptrs1[i++] = &mi1->query();
            }
        }

        Owned<IPropertyTreeIterator> mi2 = t2->getElements("*");
        if(mi2.get())
        {
            i = 0;
            for(mi2->first(); mi2->isValid(); mi2->next())
            {
                ptrs2[i++] = &mi2->query();
            }
        }

        for(i = 0; i < num1; i++)
        {
            if(!ptrs1[i])
                continue;
            const char* name1 = ptrs1[i]->queryName();
            bool foundmatch = false;
            for(int j = 0; j < num2; j++)
            {
                if(ptrs2[j])
                {
                    const char* name2 = ptrs2[j]->queryName();
                    if((name1 == name2) || (name1 && name2 && strcmp(name1, name2) == 0))
                    {
                        StringBuffer xpathbuf(xpath);
                        xpathbuf.append("/").append(name1);

                        StringBuffer xpathFullBuf(xpathFull);
                        xpathFullBuf.append("/").append(name1);
                        StringBuffer attrString;
                        getAttrString(ptrs2[j], attrString);
                        if(attrString.length())
                            xpathFullBuf.appendf("[%s]", attrString.str());

                        if(cmpPtree(xpathbuf.str(), ptrs1[i], ptrs2[j], xpathFullBuf.str()))
                        {
                            ptrs1[i] = NULL;
                            ptrs2[j] = NULL;
                            foundmatch = true;
                            break;
                        }
                    }
                }
            }

            if(!foundmatch)
            {
                m_compcache.setValue(key.c_str(),false);
                delete[] ptrs1;
                delete[] ptrs2;
                return false;
            }
        }

        for(i = 0; i < num1; i++)
        {
            if(ptrs1[i] != NULL)
            {
                m_compcache.setValue(key.c_str(),false);
                return false;           
            }
        }

        for(i = 0; i < num2; i++)
        {
            if(ptrs2[i] != NULL)
            {
                m_compcache.setValue(key.c_str(),false);
                return false;
            }
        }

        delete[] ptrs1;
        delete[] ptrs2;

        m_compcache.setValue(key.c_str(),true);
        return true;
    }
    else if((t1->hasChildren() && !t2->hasChildren()) || (!t1->hasChildren() && t2->hasChildren()))
    {
        m_compcache.setValue(key.c_str(),false);
        return false;
    }
    else
    {
        const char* val1 = t1->queryProp(".");
        const char* val2 = t2->queryProp(".");

        bool isEqual = false;
        StringBuffer keyBuf;
        if((val1 && val2 && strcmp(val1, val2) != 0) || (val1 && !val2) || (!val1 && val2))
            isEqual = false;
        else
            isEqual = true;

        m_compcache.setValue(key.c_str(),isEqual);
        return isEqual;
    }
}   

int CXmlDiff::countLeaves(IPropertyTree* t)
{
    if(!t)
        return 0;

    if(!t->hasChildren())
        return 1;

    int leaves = 0;

    Owned<IPropertyTreeIterator> mi = t->getElements("*");
    if(mi.get())
    {
        for(mi->first(); mi->isValid(); mi->next())
        {
            leaves += countLeaves(&mi->query());
        }
    }

    return leaves;
}

// Count the difference of 2 trees in terms of number of different leaves.
int CXmlDiff::countDiff(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull)
{
    //printf("countDiff %s\n", xpath);
    if(t1 == t2)
        return 0;

    StringBuffer keybuf;
    keybuf.appendf("%p-%p", t1, t2);
    std::string key = keybuf.str();
    if(m_diffcountcache.find(key) != m_diffcountcache.end())
    {
        return m_diffcountcache[key];
    }

    if(xpath && *xpath)
    {
        if(m_ignoredXPaths.find(xpath) != m_ignoredXPaths.end())
        {
            m_diffcountcache[key] = 0;
            return 0;
        }
    }

    if(!t1 || !t2)
    {
        if(!t1)
        {
            if(t2)
            {
                int diffcount = countLeaves(t2);
                m_diffcountcache[key] = diffcount;
                return diffcount;
            }
            else
                return 0;
        }
        else
        {
            int diffcount = countLeaves(t1);
            m_diffcountcache[key] = diffcount;
            return diffcount;
        }
    }

    const char* name1 = t1->queryName();
    const char* name2 = t2->queryName();
    if((name1 && !name2) || (!name1 && name2) || (name1 && name2 && strcmp(name1, name2) != 0))
    {
        int diffcount = countLeaves(t1) + countLeaves(t2);
        m_diffcountcache[key] = diffcount;
        return diffcount;
    }

    int diffcount = 0;
    if(t1->hasChildren() && t2->hasChildren())
    {
        int num1 = t1->numChildren();
        int num2 = t2->numChildren();
        IPropertyTree** ptrs1 = new IPropertyTree*[num1];
        IPropertyTree** ptrs2 = new IPropertyTree*[num2];
        int i;
        for(i = 0; i < num1; i++)
            ptrs1[i] = NULL;
        for(i = 0; i < num2; i++)
            ptrs2[i] = NULL;

        Owned<IPropertyTreeIterator> mi1 = t1->getElements("*");
        if(mi1.get())
        {
            i = 0;
            for(mi1->first(); mi1->isValid(); mi1->next())
            {
                ptrs1[i++] = &mi1->query();
            }
        }

        Owned<IPropertyTreeIterator> mi2 = t2->getElements("*");
        if(mi2.get())
        {
            i = 0;
            for(mi2->first(); mi2->isValid(); mi2->next())
            {
                ptrs2[i++] = &mi2->query();
            }
        }

        int chld_total = num1<=num2?num1:num2;
        int chld_diff = num1<=num2?(num2 - num1):(num1 - num2);
        int unmatched = 0;

        for(i = 0; i < num1; i++)
        {
            bool foundmatch = false;
            if(!ptrs1[i])
                continue;
            const char* name1 = ptrs1[i]->queryName();
            for(int j = 0; j < num2; j++)
            {
                if(ptrs2[j])
                {
                    const char* name2 = ptrs2[j]->queryName();
                    if(name1 && name2 && strcmp(name1, name2) == 0)
                    {
                        StringBuffer xpathbuf(xpath);
                        xpathbuf.append("/").append(name1);

                        StringBuffer xpathFullBuf(xpathFull);
                        xpathFullBuf.append("/").append(name1);
                        StringBuffer attrString;
                        getAttrString(ptrs2[j], attrString);
                        if(attrString.length())
                            xpathFullBuf.appendf("[%s]", attrString.str());

                        if(cmpPtree(xpathbuf.str(), ptrs1[i], ptrs2[j], xpathFullBuf.str()))
                        {
                            ptrs1[i] = NULL;
                            ptrs2[j] = NULL;
                            foundmatch = true;
                            break;
                        }
                    }
                }
            }

            if(!foundmatch)
            {
                unmatched++;
                if(m_difflimit > 0 && unmatched >= m_difflimit)
                {
                    int estimate = chld_total*unmatched/(i+1) + 2*chld_diff;
                    m_diffcountcache[key] = estimate;
                    return estimate;
                }
            }
        }

        int** diffmatrix = new int*[num1];
        for(i = 0; i < num1; i++)
        {
            diffmatrix[i] = new int[num2];
            for(int j = 0; j < num2; j++)
                diffmatrix[i][j] = -1;
        }

        std::vector<std::pair<int, std::pair<int, int> > > ordered_list;

        for(i = 0; i < num1; i++)
        {
            if(ptrs1[i] != NULL)
            {
                const char* name1 = ptrs1[i]->queryName();
                StringBuffer xpathbuf;
                xpathbuf.appendf("%s/%s", xpath, name1?name1:"");

                StringBuffer xpathFullBuf;
                xpathFullBuf.appendf("%s/%s", xpathFull, name1?name1:"");

                StringBuffer attrString;
                getAttrString(ptrs1[i], attrString);
                if(attrString.length())
                    xpathFullBuf.appendf("[%s]", attrString.str());

                for(int j = 0; j < num2; j++)
                {
                    if(ptrs2[j])
                    {
                        const char* name2 = ptrs2[j]->queryName();
                        if(name1 && name2 && strcmp(name1, name2) == 0)
                        {
                            diffmatrix[i][j] = countDiff(xpathbuf.str(), ptrs1[i], ptrs2[j], xpathFullBuf.str());
                            int ind = 0;
                            while(ind < ordered_list.size())
                            {
                                if(ordered_list[ind].first > diffmatrix[i][j])
                                    break;
                                ind++;
                            }
                            ordered_list.insert(ordered_list.begin() + ind, std::make_pair(diffmatrix[i][j], std::pair<int, int>(i, j)));
                        }
                    }
                }
            }
        }

        int ind = 0;
        while(ind < ordered_list.size())
        {
            int diffval = ordered_list[ind].first;
            int x = ordered_list[ind].second.first;
            int y = ordered_list[ind].second.second;
            
            if(diffval >= 0 && ptrs1[x] && ptrs2[y])
            {
                StringBuffer xpathbuf;
                const char* name = ptrs1[x]->queryName();
                xpathbuf.appendf("%s/%s", xpath, name?name:"");
            
                diffcount += diffval; //countDiff(xpathbuf.str(), ptrs1[x], ptrs2[y]);
                ptrs1[x] = NULL;
                ptrs2[y] = NULL;
            }
            ind++;
        }

        for(i = 0; i < num1; i++)
        {
            delete diffmatrix[i];
        }
        delete diffmatrix;

        for(i = 0; i < num1; i++)
        {
            if(ptrs1[i])
                diffcount += countLeaves(ptrs1[i]);
        }

        for(i = 0; i < num2; i++)
        {
            if(ptrs2[i] != NULL)
                diffcount += countLeaves(ptrs2[i]);
        }

        delete[] ptrs1;
        delete[] ptrs2;
    }
    else if((t1->hasChildren() && !t2->hasChildren()) || (!t1->hasChildren() && t2->hasChildren()))
        diffcount = countLeaves(t1) + countLeaves(t2);
    else
    {
        const char* val1 = t1->queryProp(".");
        const char* val2 = t2->queryProp(".");

        if((val1 && !val2) || (!val1 && val2) || (val1 && val2 && strcmp(val1, val2) != 0))
            diffcount = 2;
        else
            diffcount = 0;
    }

    m_diffcountcache[key] = diffcount;
    return diffcount;
}   

bool CXmlDiff::diffPtree(const char* xpath, IPropertyTree* t1, IPropertyTree* t2, const char* xpathFull)
{
    if(xpath && *xpath)
    {
        if(m_ignoredXPaths.find(xpath) != m_ignoredXPaths.end())
            return true;
    }

    if(t1 == t2)
        return true;

    if(!t1 || !t2)
    {
        if(!t1)
        {
            if(t2)
            {
                printPtree("> ", xpath, t2, xpathFull);
                return false;
            }
            else
                return true;
        }
        else
        {
            printPtree("< ", xpath, t1, xpathFull);
            return false;
        }
    }

    const char* name1 = t1->queryName();
    const char* name2 = t2->queryName();    
    // If their tag names are not the same, print out the whole trees as their difference.
    if((name1 && !name2) || (!name1 && name2) || (name1 && name2 && strcmp(name1, name2) != 0))
    {
        printPtree("< ", xpath, t1, xpathFull);
        printPtree("> ", xpath, t2, xpathFull);
        return false;
    }

    StringBuffer initialPath, initialPathFull;
    if(!xpath || !*xpath)
    {
        initialPath.appendf("%s", name1);
        xpath = initialPath.str();
    }
    if(!xpathFull || !*xpathFull)
    {
        initialPathFull.appendf("%s", name1);
        xpathFull = initialPathFull.str();
    }

    bool isEqual = true;

  // compare attrs
    if (cmpAttributes(t1,t2,xpathFull,true))
        isEqual = false;

    if(t1->hasChildren() && t2->hasChildren())
    {
        int num1 = t1->numChildren();
        int num2 = t2->numChildren();
        if(num1 != num2)
        {
            isEqual = false;
        }
        IPropertyTree** ptrs1 = new IPropertyTree*[num1];
        IPropertyTree** ptrs2 = new IPropertyTree*[num2];
        int i;
        for(i = 0; i < num1; i++)
            ptrs1[i] = NULL;
        for(i = 0; i < num2; i++)
            ptrs2[i] = NULL;

        Owned<IPropertyTreeIterator> mi1 = t1->getElements("*");
        if(mi1.get())
        {
            i = 0;
            for(mi1->first(); mi1->isValid(); mi1->next())
            {
                ptrs1[i++] = &mi1->query();
            }
        }

        Owned<IPropertyTreeIterator> mi2 = t2->getElements("*");
        if(mi2.get())
        {
            i = 0;
            for(mi2->first(); mi2->isValid(); mi2->next())
            {
                ptrs2[i++] = &mi2->query();
            }
        }

        for(i = 0; i < num1; i++)
        {
            if(!ptrs1[i])
                continue;
            const char* name1 = ptrs1[i]->queryName();
            for(int j = 0; j < num2; j++)
            {
                if(ptrs2[j])
                {
                    const char* name2 = ptrs2[j]->queryName();
                    if(name1 && name2 && strcmp(name1, name2) == 0)
                    {
                        StringBuffer xpathbuf(xpath);
                        xpathbuf.append("/").append(name1);
                        
                        StringBuffer xpathFullBuf(xpathFull);
                        xpathFullBuf.append("/").append(name1);
                        StringBuffer attrString;
                        getAttrString(ptrs2[j], attrString);
                        if(attrString.length())
                            xpathFullBuf.appendf("[%s]", attrString.str());

                        bool isEqual = cmpPtree(xpathbuf.str(), ptrs1[i], ptrs2[j], xpathFullBuf.str());
                        if(isEqual)
                        {
                            ptrs1[i] = NULL;
                            ptrs2[j] = NULL;
                            break;
                        }
                    }
                }
            }
        }
        
        int cost = 0;

        //calculate cost only if m_difflimit is non-zero
        if (m_difflimit)
            for(i = 0; i < num1; i++)
            {
                if(ptrs1[i])
                {
                    const char* name1 = ptrs1[i]->queryName();
                    if(!name1 || !*name1)
                        continue;
                    for(int j = 0; j < num2; j++)
                    {
                        if(ptrs2[j])
                        {
                            const char* name2 = ptrs2[j]->queryName();
                            if(name2 && strcmp(name1, name2) == 0)
                                cost++;
                        }
                    }
                }
            }

        if(m_ooo && (m_difflimit == 0 || cost < m_difflimit*m_difflimit))
        {
            int** diffmatrix = new int*[num1];
            for(i = 0; i < num1; i++)
            {
                diffmatrix[i] = new int[num2];
                for(int j = 0; j < num2; j++)
                    diffmatrix[i][j] = -1;
            }

            std::vector<std::pair<int, std::pair<int, int> > > ordered_list;

            for(i = 0; i < num1; i++)
            {
                if(ptrs1[i] != NULL)
                {
                    isEqual = false;

                    const char* name1 = ptrs1[i]->queryName();
                    StringBuffer xpathbuf;
                    xpathbuf.appendf("%s/%s", xpath, name1?name1:"");

                    StringBuffer xpathFullBuf;
                    xpathFullBuf.appendf("%s/%s", xpathFull, name1?name1:"");
                    StringBuffer attrString;
                    getAttrString(ptrs1[i], attrString);
                    if(attrString.length())
                        xpathFullBuf.appendf("[%s]", attrString.str());

                    for(int j = 0; j < num2; j++)
                    {
                        if(ptrs2[j])
                        {
                            const char* name2 = ptrs2[j]->queryName();
                            if(name1 && name2 && strcmp(name1, name2) == 0)
                            {
                                //printf("counting diff %s\n", xpathbuf.str());
                                diffmatrix[i][j] = countDiff(xpathbuf.str(), ptrs1[i], ptrs2[j],xpathFullBuf.str());
                                //printf("finished counting diff %s\n", xpathbuf.str());
                                int ind = 0;
                                while(ind < ordered_list.size())
                                {
                                    if(ordered_list[ind].first > diffmatrix[i][j])
                                        break;
                                    ind++;
                                }
                                ordered_list.insert(ordered_list.begin() + ind, std::make_pair(diffmatrix[i][j], std::pair<int, int>(i, j)));
                            }
                        }
                    }
                }
            }

            int ind = 0;
            while(ind < ordered_list.size())
            {
                int diffval = ordered_list[ind].first;
                int x = ordered_list[ind].second.first;
                int y = ordered_list[ind].second.second;
                if(diffval >= 0 && ptrs1[x] && ptrs2[y])
                {
                    StringBuffer xpathbuf;
                    const char* name = ptrs1[x]->queryName();
                    xpathbuf.appendf("%s/%s", xpath, name?name:"");

                    StringBuffer xpathFullBuf;
                    xpathFullBuf.appendf("%s/%s", xpathFull, name?name:"");
                    StringBuffer attrString;
                    getAttrString(ptrs1[x], attrString);
                    if(attrString.length())
                        xpathFullBuf.appendf("[%s]", attrString.str());

                    diffPtree(xpathbuf.str(), ptrs1[x], ptrs2[y], xpathFullBuf.str());
                    ptrs1[x] = NULL;
                    ptrs2[y] = NULL;
                }
                ind++;
            }

            for(i = 0; i < num1; i++)
            {
                delete diffmatrix[i];
            }
            delete diffmatrix;
        }
        else
        {
            for(i = 0; i < num1; i++)
            {
                if(ptrs1[i])
                {
                    const char* name1 = ptrs1[i]->queryName();
                    if(!name1 || !*name1)
                        continue;
                    for(int j = 0; j < num2; j++)
                    {
                        if(ptrs2[j])
                        {
                            const char* name2 = ptrs2[j]->queryName();
                            if(name2 && strcmp(name1, name2) == 0)
                            {
                                StringBuffer xpathbuf;
                                xpathbuf.appendf("%s/%s", xpath, name2);

                                StringBuffer xpathFullBuf;
                                xpathFullBuf.appendf("%s/%s", xpathFull, name2?name2:"");
                                StringBuffer attrString;
                                getAttrString(ptrs2[j], attrString);
                                if(attrString.length())
                                    xpathFullBuf.appendf("[%s]", attrString.str());
                                
                                diffPtree(xpathbuf.str(), ptrs1[i], ptrs2[j], xpathFullBuf.str());
                                ptrs1[i] = NULL;
                                ptrs2[j] = NULL;
                                break;
                            }
                        }
                    }
                }
            }

        }

        for(i = 0; i < num1; i++)
        {
            if(ptrs1[i])
            {
                isEqual = false;
                StringBuffer xpathbuf;
                const char* name = ptrs1[i]->queryName();
                xpathbuf.appendf("%s/%s", xpath, name?name:"");

                StringBuffer xpathFullBuf;
                xpathFullBuf.appendf("%s/%s", xpathFull, name?name:"");
                StringBuffer attrString;
                getAttrString(ptrs1[i], attrString);
                if(attrString.length())
                    xpathFullBuf.appendf("[%s]", attrString.str());

                printPtree("< ", xpathbuf.str(), ptrs1[i], xpathFullBuf.str());
            }
        }

        for(i = 0; i < num2; i++)
        {
            if(ptrs2[i] != NULL)
            {
                isEqual = false;
                StringBuffer xpathbuf;
                const char* name = ptrs2[i]->queryName();
                xpathbuf.appendf("%s/%s", xpath, name?name:"");

                StringBuffer xpathFullBuf;
                xpathFullBuf.appendf("%s/%s", xpathFull, name?name:"");
                StringBuffer attrString;
                getAttrString(ptrs2[i], attrString);
                if(attrString.length())
                    xpathFullBuf.appendf("[%s]", attrString.str());

                printPtree("> ", xpathbuf.str(), ptrs2[i], xpathFullBuf.str());
            }
        }

        delete[] ptrs1;
        delete[] ptrs2;

        return isEqual;
    }
    else if((t1->hasChildren() && !t2->hasChildren()) || (!t1->hasChildren() && t2->hasChildren()))
    {
        StringBuffer xpathFullBuf(xpathFull);
        StringBuffer attrString;
        getAttrString(t1, attrString);
        if(attrString.length())
            xpathFullBuf.appendf("[%s]", attrString.str());

        printPtree("< ", xpath, t1, xpathFullBuf);

        xpathFullBuf.clear().append(xpathFull);
        getAttrString(t2, attrString);
        if(attrString.length())
            xpathFullBuf.appendf("[%s]", attrString.str());
        
        printPtree("> ", xpath, t2, xpathFullBuf.str());
        return false;
    }
    else
    {
        const char* val1 = t1->queryProp(".");
        const char* val2 = t2->queryProp(".");

        if((val1 && !val2) || (!val1 && val2) || (val1 && val2 && strcmp(val1, val2) != 0))
        {
            printDiff("< %s=%s\n", xpathFull, val1);
            printDiff("> %s=%s\n", xpathFull, val2);
            return false;
        }
        else
            return isEqual;
    }

  return isEqual;
}   

void CXmlDiff::printPtree(const char* prefix, const char* xpath, IPropertyTree* t, const char* xpathFull)
{
    if(!t)
        return;

    if(xpath && *xpath)
    {
        if(m_ignoredXPaths.find(xpath) != m_ignoredXPaths.end())
            return;
    }

    StringBuffer initialPath, initialPathFull;
    if(!xpath || !*xpath)
    {
        initialPath.appendf("%s", t->queryName());
        xpath = initialPath.str();
    }
    if(!xpathFull || !*xpathFull)
    {
        initialPathFull.appendf("%s", t->queryName());
        StringBuffer attrString;
        getAttrString(t, attrString);
        if(attrString.length())
            initialPathFull.appendf("[%s]", attrString.str());
        xpathFull = initialPathFull.str();
    }

    if(t->hasChildren())
    {
        Owned<IPropertyTreeIterator> mi = t->getElements("*");
        if(mi.get())
        {
            for(mi->first(); mi->isValid(); mi->next())
            {
                StringBuffer xpathbuf(xpath);
                xpathbuf.append("/").append(mi->query().queryName());

                StringBuffer xpathFullBuf(xpathFull);
                xpathFullBuf.append("/").append(mi->query().queryName());
                StringBuffer attrString;
                getAttrString(&mi->query(), attrString);
                if(attrString.length())
                    xpathFullBuf.appendf("[%s]", attrString.str());

                printPtree(prefix, xpathbuf.str(), &mi->query(), xpathFullBuf.str());
            }
        }
    }
    else
    {
        const char* val = t->queryProp(".");
        printDiff("%s%s=%s\n", prefix, xpathFull, val?val:"");
    }
}

void CXmlDiff::printDiff(const char *fmt, ...)
{
    if(!m_filenamesPrinted)
    {
        printf("%s <> %s\n", m_left.str(), m_right.str());
        m_filenamesPrinted = true;
    }

    va_list marker;
    va_start(marker, fmt);
    printf("    ");
    vfprintf(stdout, fmt, marker);
    va_end(marker);
}

