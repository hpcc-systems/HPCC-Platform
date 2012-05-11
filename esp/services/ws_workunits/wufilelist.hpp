/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#ifndef WUFILE_LIST_HPP
#define WUFILE_LIST_HPP

#include "jlib.hpp"
#include "workunit.hpp"
#include "dadfs.hpp"
#include "dfuutil.hpp"

#define WuRefFileNone           0x000
#define WuRefFileIndex          0x001
#define WuRefFileNotOnCluster   0x002
#define WuRefFileNotFound       0x004
#define WuRefFileRemote         0x008
#define WuRefFileForeign        0x010
#define WuRefFileSuper          0x020
#define WuRefSubFile            0x040
#define WuRefFileCopyInfoFailed 0x080

interface IWuReferencedFile : extends IInterface
{
    virtual const char *getLogicalName()=0;
    virtual unsigned getFlags()=0;
    virtual const SocketEndpoint &getForeignIP()=0;
    virtual void resolve(const char *cluster, IUserDescriptor *user, INode *remote, bool checkLocalFirst, StringArray *subfiles)=0;
    virtual void cloneInfo(IDFUhelper *helper, IUserDescriptor *user, INode *remote, const char *cluster, bool overwrite=false)=0;
    virtual void cloneSuperInfo(IUserDescriptor *user, INode *remote, bool overwrite=false)=0;
};


interface IWuReferencedFileIterator : extends IIteratorOf<IWuReferencedFile> { };

interface IWuReferencedFileList : extends IInterface
{
    virtual IWuReferencedFileIterator *getFiles()=0;
    virtual void resolveFiles(const char *process, bool checkLocalFirst, bool addSubFiles)=0;
    virtual void cloneAllInfo(bool overwrite, bool cloneSuperInfo)=0;
    virtual void cloneFileInfo(bool overwrite, bool cloneSuperInfo)=0;
    virtual void cloneRelationships()=0;
};

IWuReferencedFileList *createReferencedFileList(IConstWorkUnit *cw, const char *remoteIP, const char *user, const char *pw);

#endif //WUFILE_LIST_HPP
