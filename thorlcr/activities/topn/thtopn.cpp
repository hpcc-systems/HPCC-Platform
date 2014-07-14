/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "jiface.hpp"
#include "eclhelper.hpp"
#include "thtopn.ipp"

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
    virtual void init()
    {
        CMasterActivity::init();

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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
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
