/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _KEYDIFF_INCL
#define _KEYDIFF_INCL

#ifdef JHTREE_EXPORTS
#define jhtree_decl DECL_EXPORT
#else
#define jhtree_decl DECL_IMPORT
#endif

#include "jstring.hpp"
#include "jio.hpp"
#include "keybuild.hpp"

class IKeyDiffProgressCallback : public IInterface
{
public:
    virtual void handle(offset_t bytesRead) = 0;
};

class IKeyDiffGenerator : public IInterface
{
public:
    virtual void run() = 0;
    virtual void logStats() const = 0;
    virtual unsigned queryPatchCRC() = 0;
    virtual unsigned queryPatchFileCRC() = 0; // this should be actual crc of physical file generated
    virtual void setProgressCallback(IKeyDiffProgressCallback * callback, offset_t freq) = 0;
};

class INodeSender : public IInterface
{
public:
    virtual void send(CNodeInfo & info) = 0;
};

class INodeReceiver : public IInterface
{
public:
    virtual bool recv(CNodeInfo & info) = 0;
    virtual void stop() = 0;
};

class IKeyDiffApplicator : public IInterface
{
public:
    virtual void setTransmitTLK(INodeSender * sender) = 0;
    virtual void setReceiveTLK(INodeReceiver * receiver, unsigned numParts) = 0;
    virtual void run() = 0;
    virtual void getHeaderVersionInfo(unsigned short & versionMajor, unsigned short & versionMinor, unsigned short & minPatchVersionMajor, unsigned short & minPatchVersionMinor)= 0;
    virtual void getHeaderFileInfo(StringAttr & oldindex, StringAttr & newindex, bool & tlkInfo, StringAttr & newTLK) = 0;
    virtual bool compatibleVersions(StringBuffer & error) const = 0;
    virtual void setProgressCallback(IKeyDiffProgressCallback * callback, offset_t freq) = 0;
};

extern jhtree_decl IKeyDiffGenerator * createKeyDiffGenerator(char const * oldIndex, char const * newIndex, char const * patch, char const * newTLK, bool overwrite, unsigned compmode);
extern jhtree_decl IKeyDiffApplicator * createKeyDiffApplicator(char const * patch, bool overwrite, bool ignoreTLK);
extern jhtree_decl IKeyDiffApplicator * createKeyDiffApplicator(char const * patch, char const * oldIndex, char const * newIndex, char const * newTLK, bool overwrite, bool ignoreTLK);
extern jhtree_decl StringBuffer & getKeyDiffVersion(StringBuffer & buff);
extern jhtree_decl StringBuffer & getKeyDiffMinDiffVersionForPatch(StringBuffer & buff);
extern jhtree_decl StringBuffer & getKeyDiffMinPatchVersionForDiff(StringBuffer & buff);

// KeyReader and KeyWriter

interface IPropertyTree;


interface IKeyFileRowReader: extends IRowStream
{
    virtual IPropertyTree *queryHeader()=0;
};

interface IKeyFileRowWriter: extends IRowWriter
{
};


extern jhtree_decl IKeyFileRowReader *createKeyFileRowReader(const char *filename);
extern jhtree_decl IKeyFileRowWriter *createKeyFileRowWriter(const char *filename,IPropertyTree *header, bool overwrite);


#endif
