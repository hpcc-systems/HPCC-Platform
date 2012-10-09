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

#include "jfile.hpp"
#include "jlog.hpp"

#include "thorxmlread.hpp"

class CMatchItem : public CInterface
{
public:
    StringAttr xpath;
    unsigned size;
    CMatchItem(const char *_xpath, unsigned _size) : xpath(_xpath), size(_size) { }
};

class CParseStackInfo2 : public CInterface
{
public:
    CParseStackInfo2(const char *_tag, unsigned _index) : tag(_tag), index(_index), startOffset(0) { }
    StringAttr tag;
    unsigned index;
    offset_t startOffset;
};

class CXMLParse2 : public CInterface, implements IXMLParse
{
    Owned<IPullXMLReader> xmlReader;
    XmlReaderOptions xmlOptions;

    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        CIArrayOf<CParseStackInfo2> stack;
        unsigned level;
        StringAttr lastAtLevel;
        unsigned lastAtLevelCount;

        CIArrayOf<CMatchItem> arr;

        static int _sortF(CInterface **_left, CInterface **_right)
        {
            CMatchItem **left = (CMatchItem **)_left;
            CMatchItem **right = (CMatchItem **)_right;
            return ((*right)->size - (*left)->size);
        }
    public:
        IMPLEMENT_IINTERFACE;

        CParse()
        {
            lastAtLevelCount = 0;
        }
        ~CParse()
        {
        }
        void init()
        {
            level = 0;
        }
        void reset()
        {
            level = 0;
            stack.kill();
        }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            bool e = lastAtLevel.length() && (0 == strcmp(tag, lastAtLevel));
            if (e)
                lastAtLevelCount++;
            else
                lastAtLevelCount = 0;
            lastAtLevel.clear();

            CParseStackInfo2 *stackInfo = new CParseStackInfo2(tag, lastAtLevelCount);
            stackInfo->startOffset = startOffset;
            stack.append(*stackInfo);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
        }
        virtual void beginNodeContent(const char *tag)
        {
            level++;
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            --level;
            CParseStackInfo2 &stackInfo = stack.tos();

            StringBuffer xpath("/");
            ForEachItemIn(s, stack)
            {
                CParseStackInfo2 &sI = stack.item(s);
                xpath.append(sI.tag);
                if (sI.index)
                    xpath.append('[').append(sI.index+1).append(']');
                if (s < stack.ordinality()-1)
                    xpath.append('/');
            }

            arr.append(*new CMatchItem(xpath, endOffset-stackInfo.startOffset));
            
            lastAtLevel.set(stackInfo.tag);
            stack.pop();
        }

        void printResults()
        {
            arr.sort(_sortF);
            ForEachItemIn(m, arr)
            {
                CMatchItem &match = arr.item(m);

                PROGLOG("xpath=%s, size=%d", match.xpath.get(), match.size);
            }
        }
    } *parser;

public:
    IMPLEMENT_IINTERFACE;

    CXMLParse2(const char *fName, XmlReaderOptions _xmlOptions=xr_none) : xmlOptions(_xmlOptions) { init(); go(fName); }
    ~CXMLParse2() { ::Release(parser); }
    void init()
    {
        parser = new CParse();
        parser->init();
    }

    void go(const char *fName) 
    {
        OwnedIFile ifile = createIFile(fName);
        OwnedIFileIO ifileio = ifile->open(IFOread);
        if (!ifileio)
            throw MakeStringException(0, "Failed to open: %s", ifile->queryFilename());
        Owned<IIOStream> stream = createIOStream(ifileio);
        xmlReader.setown(createPullXMLStreamReader(*stream, *parser, xmlOptions));
    }

    void printResults()
    {
        parser->printResults();
    }

// IXMLParse
    virtual bool next()
    {
        return xmlReader->next();
    }

    virtual void reset()
    {
        parser->reset();
        xmlReader->reset();
    }
};

void usage(char *prog)
{
    PROGLOG("usage: %s <filename>", prog);
}

int main(int argc, char **argv)
{
    InitModuleObjects();
    try
    {
        if (argc>=2)
        {
            Owned<CXMLParse2> parser = new CXMLParse2(argv[1]);
            while (parser->next())
                ;
            parser->printResults();
        }
        else
            usage(argv[0]);
    }
    catch (IException *e)
    {
        EXCLOG(e, "error");
        e->Release();
    }
    return 0;
}
