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

#ifndef DFUUTIL_HPP
#define DFUUTIL_HPP

#include "jstring.hpp"
#include "dfuwu.hpp"

interface IUserDescriptor;

interface IDfuFileCopier: extends IInterface
{
    virtual bool copyFile(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser=NULL,IUserDescriptor *user=NULL) = 0;
    virtual bool wait()=0; // waits for all outstanding copies to complete
};


interface IDFUhelper: extends IInterface
{
    virtual void addSuper(const char *superfname, unsigned numtoadd=0, const char **subfiles=NULL, const char *before=NULL, IUserDescriptor *user=NULL) = 0;
    virtual void removeSuper(const char *superfname, unsigned numtodelete=0, const char **subfiles=NULL, bool delsub=false, IUserDescriptor *user=NULL) = 0;
    virtual void listSubFiles(const char *superfname,StringAttrArray &out, IUserDescriptor *user=NULL) = 0;
    virtual StringBuffer &getFileXML(const char *lfn,StringBuffer &out, IUserDescriptor *user=NULL) = 0;
    virtual void addFileXML(const char *lfn,const StringBuffer &xml, IUserDescriptor *user=NULL) = 0;
    virtual void addFileRemote(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser=NULL,IUserDescriptor *user=NULL) = 0;
    virtual void superForeignCopy(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser,IUserDescriptor *user, bool overwrite, IDfuFileCopier *copier) = 0;

    virtual void createFileClone(
                     const char *srcname,               // src LFN (can be foreign)
                     const char *cluster1,              // group name of roxie cluster
                     DFUclusterPartDiskMapping clustmap, // how the nodes are mapped
                     bool repeattlk,                    // repeat last part on all nodes if key
                     const char *cluster2,              // servers cluster (for just tlk)
                     IUserDescriptor *userdesc,         // user desc for local dali
                     const char *foreigndali,           // can be omitted if srcname foreign or local
                     IUserDescriptor *foreignuserdesc,  // user desc for foreign dali if needed
                     const char *nameprefix,            // prefix for new name
                     bool overwrite,                    // delete destination if exists
                     bool dophysicalcopy=false              // NB *not* using DFU server (so use with care)
                     ) = 0;


    virtual void createSingleFileClone(const char *srcname,             // src LFN (can't be super)
                         const char *dstname,               // dst LFN
                         const char *cluster1,              // group name of roxie cluster
                         DFUclusterPartDiskMapping clustmap, // how the nodes are mapped
                         bool repeattlk,                    // repeat last part on all nodes if key
                         const char *cluster2,              // servers cluster (for just tlk)
                         IUserDescriptor *userdesc,         // user desc for local dali
                         const char *foreigndali,           // can be omitted if srcname foreign or local
                         IUserDescriptor *foreignuserdesc,  // user desc for foreign dali if needed
                         bool overwrite,                    // delete destination if exists
                         bool dophysicalcopy=false          // NB *not* using DFU server (so use with care)
                         ) = 0;

    virtual void cloneFileRelationships(
        const char *foreigndali,        // where src relationships are retrieved from (can be NULL for local)
        StringArray &srcfns,            // file names on source
        StringArray &dstfns,            // corresponding filenames on dest (must exist otherwise ignored)
        IPropertyTree *relationships    // if not NULL, tree will have all relationships filled in
    ) = 0;

};

IDFUhelper *createIDFUhelper();


#endif
