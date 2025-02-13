#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jsuperhash.hpp"
#include "jregexp.hpp"
#include "jfile.hpp"


#include "sautil.hpp"

unsigned planesToGroups(const char *cmplst, StringArray &cnames, StringArray &groups)
{
    StringBuffer plane;
    const char *lastCopied = cmplst;
    while (cmplst&&*cmplst)
    {
        if (*cmplst==',')
        {
            plane.append(cmplst-lastCopied, lastCopied);
            assertex(getStoragePlane(plane.str()));
            lastCopied = cmplst+1;
            cnames.append(plane.str());
            groups.append(plane.str());
            plane.clear();
        }
        cmplst++;
    }
    if (lastCopied != cmplst) {
        plane.append(cmplst-lastCopied, lastCopied);
        assertex(getStoragePlane(plane.str()));
        cnames.append(plane.str());
        groups.append(plane.str());
    }
    return groups.ordinality();
}

unsigned clustersToGroups(IPropertyTree *envroot,const StringArray &cmplst,StringArray &cnames,StringArray &groups,bool *done)
{
    if (!envroot)
        return 0;
    for (int roxie=0;roxie<2;roxie++) {
        Owned<IPropertyTreeIterator> clusters= envroot->getElements(roxie?"RoxieCluster":"ThorCluster");
        unsigned ret = 0;
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *name = cluster.queryProp("@name");
            if (name&&*name) {
                ForEachItemIn(i,cmplst) {
                    const char *s = cmplst.item(i);
                    assertex(s);
                    if ((strcmp(s,"*")==0)||WildMatch(name,s,true)) {
                        const char *group = cluster.queryProp("@nodeGroup");
                        if (!group||!*group)
                            group = name;
                        bool found = false;
                        ForEachItemIn(j,groups) 
                            if (strcmp(groups.item(j),group)==0)
                                found = true;
                        if (!found) {
                            cnames.append(name);
                            groups.append(group);
                            if (done)
                                done[i] =true;
                            break;
                        }
                    }
                }
            }
        }
    }
    return groups.ordinality();
}

unsigned clustersToGroups(IPropertyTree *envroot,const StringArray &cmplst,StringArray &groups,bool *done)
{
    StringArray cnames;
    return clustersToGroups(envroot,cmplst,cnames,groups,done);
}
