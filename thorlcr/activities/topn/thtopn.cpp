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

#include "jiface.hpp"

#include "eclhelper.hpp"

#include "thtopn.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/topn/thtopn.cpp $ $Id: thtopn.cpp 62376 2011-02-04 21:59:58Z sort $");

#define MERGE_GRANULARITY 4

class CTopNActivityMaster : public CMasterActivity
{
    MemoryBuffer *sD;
public:
    CTopNActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        sD = NULL;
        mpTag = container.queryJob().allocateMPTag();
    }
    ~CTopNActivityMaster()
    {
        if (sD) delete [] sD;
    }
    void init()
    {
        // prepare topology.

        unsigned rootNodes = container.queryJob().querySlaves();

        unsigned res = MERGE_GRANULARITY;

        sD = new MemoryBuffer[rootNodes];
        UnsignedArray nodes1, nodes2;
        UnsignedArray *currentLevel = &nodes1, *nextLevel = &nodes2;
        unsigned n = 0; while (n<rootNodes) currentLevel->append(n++);
        while (rootNodes > 1)
        {
            assertex(rootNodes);

            unsigned r = (rootNodes+(res-1))/res; // groups
            unsigned t = 0;
            n = 0;
            bool first = true;
            loop
            {
                if (first)
                {
                    first = false;
                    nextLevel->append(currentLevel->item(n));
                }
                else
                {
#ifdef _DEBUG
                    unsigned node = nextLevel->tos();
                    unsigned item = currentLevel->item(n);
                    ActPrintLog("Adding to node=%d, item=%d", node, item);
#endif
                    sD[nextLevel->tos()].append(currentLevel->item(n));
                }
                n++;
                if (n>=rootNodes) break;
                t += r;
                if (t>=rootNodes)
                {
                    t -= rootNodes;
                    first = true;
                }
            }
            
            assertex(sD[nextLevel->tos()].length()); // something must have been added
            n = 0;
            while (n<nextLevel->ordinality()) sD[nextLevel->item(n++)].append(0); // terminator
#ifdef _DEBUG
            ActPrintLog("EOL");
#endif
            rootNodes = nextLevel->ordinality();

            UnsignedArray *tmp = currentLevel;
            currentLevel = nextLevel;
            nextLevel = tmp;
            nextLevel->kill();
        }
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        serializeMPtag(dst, mpTag);
        dst.append(sD[slave].length());
        dst.append(sD[slave].length(), sD[slave].toByteArray());
    }
};


CActivityBase *createTopNActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CTopNActivityMaster(container);
}
