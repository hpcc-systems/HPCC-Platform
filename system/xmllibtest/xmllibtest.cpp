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

#include "jliball.hpp"

#include <stdio.h>
#include <time.h>

#include "xslprocessor.hpp"
#include "xmlvalidator.hpp"

//--------------------------------------------------------------------------------
#define SEISINT_NAMESPACE "http://seisint.com"

//#define TEST_SOCKET_OUTPUT

//--------------------------------------------------------------------------------
// globals
bool gUseThread = false;
bool gDoHelp = false;
bool gStreamingBySocket = false;
int  gArgc = 0;
char** gArgv = NULL;
int  gPort = 9000;
const char* gHost = "localhost";
const char* gTargetNamespace = NULL;
bool gByStringBuffer = false;

enum TestType { Type_None, Type_XsltTransform, Type_XmlValidation };
TestType gTestType = Type_XsltTransform;

void doTransform();
void doTransformUseThead();
void doXmlValidation();
void usage();
void parseCommandLineOptions(int argc, char** argv);

//--------------------------------------------------------------------------------
// main

int main(int argc, char** argv) 
{
    parseCommandLineOptions(argc, argv);
    if (gDoHelp) 
        usage();
    
    InitModuleObjects();
    
    switch(gTestType)
    {
    case Type_XsltTransform:
        if (gUseThread)
            doTransformUseThead();
        else
            doTransform();
        break;
        
    case Type_XmlValidation:
        doXmlValidation();
        break;
        
    default: usage();
        break;
    }
    
    delete[] gArgv;
    
    ExitModuleObjects();
    releaseAtoms();

    return 0;
}

void usage()
{
    puts("Usage:");
    printf(" Xslt Transform: %s [options] [<xml-file-name> <xsl-file-name> [<output-file-name>]]\n", gArgv[0]);
    printf(" XML Validation: %s -v [-b] [-ns=target-namespace] <xml-file-name> <xsd-file-name>\n", gArgv[0]);
    puts("Options:");
    puts("  -?/-h: display this usage");
    puts("  -v: do xml validation");
    puts("  -t: use threads to do the transform");
    puts("  -s: streaming the output through socket");
    puts("  -h=host: the host the socket output to [default: localhost]");
    puts("  -p=port: the port the socket output to [default: 9000]");
    puts("  -b: validating through string buffer");
    puts("If no parameter is given, a default transform is done.");
    
    exit(1);
}

void parseCommandLineOptions(int argc, char** argv)
{
    gArgc = argc;
    
    int i;
    for (i=1;i<argc;i++)
    {
        if (argv[i][0] == '-')
        {
            if (!strcmp(argv[i], "-?") || !strcmp(argv[i], "-h"))
            {
                gDoHelp = true;
                gTestType = Type_None;
            }
            else if (!strcmp(argv[i],"-t"))
                gUseThread = true;
            else if (!strcmp(argv[i],"-v"))
                gTestType = Type_XmlValidation;
            else if (!strcmp(argv[i],"-s"))
                gStreamingBySocket = true;
            else if (!strncmp(argv[i],"-h=",3))
                gHost = argv[i]+3;
            else if (!strncmp(argv[i],"-p=",3))
                gPort = atoi(argv[i]+3);
            else if (!strncmp(argv[i],"-ns=",4))
                gTargetNamespace = argv[i]+4;
            else if (!strcmp(argv[i],"-b"))
                gByStringBuffer = true;
            
            // unknown
            else {
                fprintf(stderr, "Unknown options: %s\n", argv[i]);
                exit(1);
            }               
            
            gArgc--; // remove the option for the arg list
        }
    }
    
    gArgv = new char*[gArgc];
    int idx = 0;
    for (i=0; i<argc; i++)
    {
        if (argv[i][0] != '-')
            gArgv[idx++] = argv[i];
    }
    
    assert(gArgc==idx);
}

//--------------------------------------------------------------------------------

class CIncludeHandler : public CInterface, implements IIncludeHandler
{
public:
    IMPLEMENT_IINTERFACE;
    
    virtual bool getInclude(const char* includename, MemoryBuffer& includebuf, bool& pathOnly)
    {
        pathOnly = false;
        printf("includename=%s\n", includename);
        
        /*
        struct tm *curtime;
        time_t aclock;
        time( &aclock );
        curtime = localtime( &aclock );  
        */
        
        StringBuffer incbuf;
        if(strstr(includename, "foo.xsl") != NULL)
        {
            incbuf.append("<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='1.0'>"
                "<xsl:template match='emphasis'>"
                "   <B><xsl:apply-templates/></B>"
                );
            incbuf.append("</xsl:template>").append("</xsl:stylesheet>");
        }
        else if(strstr(includename, "bar.xsl") != NULL)
        {
            incbuf.append("<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='1.0'>"
                "<xsl:template match='italics'>"
                "   <I><xsl:apply-templates/></I>"
                );
            incbuf.append("</xsl:template>").append("</xsl:stylesheet>");
        }
        
        includebuf.append(incbuf.str());
        return true;
    }
};


#define CRYPTSIZE 32
#define CRYPTKEY "Unable to find process %s version %s"

StringBuffer sSetFromXSLT;
void getString(StringBuffer& ret, const char* in, IXslTransform* pTransform)
{
    ret.clear().append(sSetFromXSLT);
}

void setString(StringBuffer& ret, const char* in, IXslTransform* pTransform)
{
    sSetFromXSLT.clear().append(in);
    ret.clear();
}

void mydecrypt(StringBuffer &ret, const char *in)
{
    if (in)
    {
        MemoryBuffer out, out1;
        JBASE64_Decode(in, out);
        aesDecrypt(CRYPTKEY, CRYPTSIZE, out.toByteArray(), out.length(), out1);
        ret.append(out1.length(), out1.toByteArray());
    }
}

//--------------------------------------------------------------------------------

class XsltThread : public Thread
{
private:
    int argc;
    char** argv;
    
public:
    XsltThread(int _argc, char** _argv)
    {
        argc = _argc;
        argv = _argv;
    }
    
    int run()
    {
        doTransform();
        return 0;
    }
};

void doTransformUseThead()
{
    const int NUMTHREADS = 1;
    
    XsltThread* t1s[NUMTHREADS];
    XsltThread* t2s[NUMTHREADS];
    XsltThread* t3s[NUMTHREADS];
    int i;
    for(i = 0; i < NUMTHREADS; i++)
    {
        t1s[i] = new XsltThread(3, gArgv);
        t2s[i] = new XsltThread(3, gArgv);
        t3s[i] = new XsltThread(3, gArgv);
    }
    for(i = 0; i < NUMTHREADS; i++)
    {
        t1s[i]->start();
        t2s[i]->start();
        t3s[i]->start();
    }
    
    for(i = 0; i < NUMTHREADS; i++)
    {
        t1s[i]->join();
        t2s[i]->join();
        t3s[i]->join();
    }
    for(i = 0; i < NUMTHREADS; i++)
    {
        delete t1s[i];
        delete t2s[i];
        delete t3s[i];
    }
}

//--------------------------------------------------------------------------------

void doTransform()
{
    Owned<IXslProcessor> processor = getXslProcessor();
    //Owned<IXslProcessor> processor2 = getXslProcessor2();
    Owned<IXslFunction> externalFunction1;
    Owned<IXslFunction> externalFunction2;
    
    if(gArgc == 3)
    {
        StringBuffer xmlbuf, xslbuf;
        try {
            xmlbuf.loadFile(gArgv[1]);
            xslbuf.loadFile(gArgv[2]);
        } catch (IException* e) {
            StringBuffer msg;
            ERRLOG("Exception caught: %s", e->errorMessage(msg).str());
            e->Release();
            return;
        }
        
        //for(int i = 0; i < 500; i++)
        {
            Owned<IXslTransform> transform = processor->createXslTransform();
            externalFunction1.setown(transform->createExternalFunction("getString", getString));
            transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction1.get(), true);
            
            externalFunction2.setown(transform->createExternalFunction("setString", setString));
            transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction2.get(), true);
            //transform->setXslSource(xslbuf.str(), xslbuf.length());
            //transform->setXmlSource(xmlbuf.str(), xmlbuf.length());
            transform->setXmlSource(gArgv[1]);
            transform->loadXslFromFile(gArgv[2]);
            try 
            {
                if (gStreamingBySocket)
                {
                    SocketEndpoint ep(gPort,gHost);
                    ISocket* s = ISocket::connect(ep);
                    transform->transform(s);
                } 
                else 
                {
                    StringBuffer buf;
                    transform->transform(buf);
                    printf("%s\n", buf.str());
                }
            }
            catch (IException* e) 
            {
                StringBuffer msg;
                ERRLOG("Exception caught: %s", e->errorMessage(msg).str());
                e->Release();
            }
        }
    }
    else if(gArgc == 4) 
    {
        //for(int i = 0; i < 500; i++)
        {
            Owned<IXslTransform> transform = processor->createXslTransform();
            externalFunction1.setown(transform->createExternalFunction("getString", getString));
            transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction1.get(), true);
            
            externalFunction2.setown(transform->createExternalFunction("setString", setString));
            
            transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction2.get(), true);
            transform->setXmlSource(gArgv[1]);
            transform->loadXslFromFile(gArgv[2]);
            transform->setResultTarget(gArgv[3]);
            
            try 
            {
                if (processor->execute(transform) < 0)
                    puts(transform->getLastError());
            } 
            catch (IException* e) 
            {
                StringBuffer msg;
                ERRLOG("Exception caught: %s", e->errorMessage(msg).str());
                e->Release();
            }
        }
        /*
        if (*transform->getMessages())
        {
        puts("Messages produces are as follows:");
        puts(transform->getMessages());
        }
        */
    }
    else 
    {
        const char* const  theInputDocument = 
            "<?xml version='1.0' encoding='ISO-8859-1'?><root><doc>Hello world!</doc><emphasis>important</emphasis><italics>interesting</italics></root>";
        
        const char* const  theStylesheet =
            "<?xml version='1.0'?>"
            "<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='1.0'>"
            "<xsl:import href='bar.xsl'/>"
            "<xsl:output encoding='US-ASCII'/>"
            "<xsl:template match='doc'>"
            "<xsl:message>ttttt</xsl:message>"
            "   <out><xsl:value-of select='.'/></out>"
            "</xsl:template>"
            "<xsl:include href='foo.xsl'/>"
            "</xsl:stylesheet>";
        
        //for(int i = 0; i < 1; i++)
        {
            Owned<IXslTransform> transform = processor->createXslTransform();
            transform->setXmlSource(theInputDocument, strlen(theInputDocument));
            transform->setIncludeHandler(new CIncludeHandler);
            // Attention - the callback getXsl is called everytime the following function is called -
            transform->setXslSource(theStylesheet, strlen(theStylesheet), "foobartest", ".");
            
            StringBuffer buf;
            
            printf("transforming ....\n");
            
            try 
            {
                transform->transform(buf);
            } 
            catch (IException* e) 
            {
                StringBuffer msg;
                ERRLOG("Exception caught: %s", e->errorMessage(msg).str());
                e->Release();
            }
            
            printf("result=\n%s\n\n", buf.str());
            printf("lastError=\n%s\n\n", transform->getLastError());
        }
    }
}

//--------------------------------------------------------------------------------
// Validation

bool loadFile(StringBuffer& s, const char* file)
{
    try {
        s.loadFile(file);
    } catch (IException* e) {
        StringBuffer msg;
        fprintf(stderr,"Exception caught: %s", e->errorMessage(msg).str());
        return false;
    }
    return true;
}

void doXmlValidation()
{
    if (gArgc != 3)
        usage();
    printf("Validating XML %s against %s...\n", gArgv[1], gArgv[2]);
    Owned<IXmlDomParser> p = getXmlDomParser();
    Owned<IXmlValidator> v = p->createXmlValidator();
    
    if (!gByStringBuffer)
    {
        if (!v->setXmlSource(gArgv[1])) 
            return;
        if (!v->setSchemaSource(gArgv[2])) 
            return;
    }
    else
    {
        StringBuffer xml, xsd;
        if (loadFile(xml, gArgv[1]) && loadFile(xsd, gArgv[2]))
        {
            v->setXmlSource(xml, strlen(xml));
            v->setSchemaSource(xsd, strlen(xsd));
        }
        else
            return;
    }
    
    if (gTargetNamespace)
        v->setTargetNamespace(gTargetNamespace);
    
    try {
        v->validate();
    } catch (IMultiException* me) {
        IArrayOf<IException> &es = me->getArray();
        for (aindex_t i=0; i<es.ordinality(); i++)
        {
            StringBuffer msg;
            IException& e = es.item(i);
            printf("Error %d: %s\n", i, e.errorMessage(msg).str());
        }       
        me->Release();
    }
}

// end
//--------------------------------------------------------------------------------
