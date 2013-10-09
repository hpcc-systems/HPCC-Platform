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

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "jlib.hpp"
#include "thorparse.hpp"
#include "parselib.hpp"

#define PARSELIB_VERSION "PARSELIB 1.0.1"

IAtom * separatorTagAtom;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    separatorTagAtom = createAtom("<separator>");
    return true;
}

//-------------------------------------------------------------------------------------------------------------------------------------------

static const char * EclDefinition =
"export ParseLib := SERVICE\n"
"   string getParseTree() : c,volatile,entrypoint='plGetDefaultParseTree',userMatchFunction; \n"
"   string getXmlParseTree() : c,volatile,entrypoint='plGetXmlParseTree',userMatchFunction; \n"
"END;";

static const char * compatibleVersions[] = {
    "PARSELIB 1.0.0 [fa9b3ab8fad8e46d8c926015cbd39f06]", 
    PARSELIB_VERSION,
    NULL };

PARSELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = PARSELIB_VERSION;
    pb->moduleName = "lib_parselib";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ParseLib PARSE helper library";
    return true;
}

namespace nsParselib {

IPluginContext * parentCtx = NULL;

static bool hasChildren(IMatchWalker * walker)
{
    for (unsigned i=0;;i++)
    {
        Owned<IMatchWalker> child = walker->getChild(i);
        if (!child)
            return false;
        if (child->queryName() != separatorTagAtom)
            return true;
    }
}

static StringBuffer & getElementText(StringBuffer & s, IMatchWalker * walker)
{
    unsigned len = walker->queryMatchSize();
    const char * text = (const char *)walker->queryMatchStart();
    return s.append(len, text);
}


static void expandElementText(StringBuffer & s, IMatchWalker * walker)
{
    getElementText(s.append('"'), walker).append('"');
}

static void getDefaultParseTree(StringBuffer & s, IMatchWalker * cur)
{
    IAtom * name = cur->queryName();
    if (name != separatorTagAtom)
    {
        if (name)
        {
            StringBuffer lname;
            lname.append(name);
            lname.toLowerCase();
            s.append(lname);
        }
        if (hasChildren(cur))
        {
            s.append("[");
            for (unsigned i=0;;i++)
            {
                Owned<IMatchWalker> child = cur->getChild(i);
                if (!child)
                    break;

                getDefaultParseTree(s, child);
                s.append(" ");
            }
            s.setLength(s.length()-1);
            s.append("]");
        }
        else
            expandElementText(s, cur);
    }
}

//---------------------------------------------------------------------------

static void getXmlParseTree(StringBuffer & s, IMatchWalker * walker, unsigned indent)
{
    IAtom * name = walker->queryName();
    if (name != separatorTagAtom)
    {
        unsigned max = walker->numChildren();
        if (!name)
        {
            if (hasChildren(walker))
            {
                for (unsigned i=0; i<max; i++)
                {
                    Owned<IMatchWalker> child = walker->getChild(i);
                    getXmlParseTree(s, child, indent);
                }
            }
            else
                getElementText(s, walker);
        }
        else
        {
            StringBuffer lname;
            lname.append(name);
            lname.toLowerCase();
            s.pad(indent).append('<').append(lname).append('>');
            if (hasChildren(walker))
            {
                s.newline();
                for (unsigned i=0; i<max; i++)
                {
                    Owned<IMatchWalker> child = walker->getChild(i);
                    getXmlParseTree(s, child, indent+1);
                }
                s.pad(indent);
            }
            else
                getElementText(s, walker);
            s.append("</").append(lname).append('>').newline();
        }
    }
}

}//namespace

using namespace nsParselib;
PARSELIB_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

PARSELIB_API void plGetXmlParseTree(IMatchWalker * walker, unsigned & len, char * & text)
{
    StringBuffer s;
    getXmlParseTree(s, walker, 0);
    len = s.length();
    text = s.detach();
}

PARSELIB_API void plGetDefaultParseTree(IMatchWalker * walker, unsigned & len, char * & text)
{
    StringBuffer s;
    getDefaultParseTree(s, walker);
    len = s.length();
    text = s.detach();
}

//-------------------------------------------------------------------------------------------------------------------------------------------

