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

#ifndef DFUUTIL_HPP
#define DFUUTIL_HPP

#include "jstring.hpp"
#include "dfuwu.hpp"

interface IUserDescriptor;

interface IDfuFileCopier: extends IInterface
{
    virtual bool copyFile(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser,IUserDescriptor *user) = 0;
    virtual bool wait()=0; // waits for all outstanding copies to complete
};

#define DALI_UPDATEF_REPLACE_FILE   0x0001
#define DALI_UPDATEF_CLONE_FROM     0x0002
#define DALI_UPDATEF_APPEND_CLUSTER 0x0004
#define DALI_UPDATEF_SUPERFILES     0x0008
#define DALI_UPDATEF_PACKAGEMAP     0x0100

#define DALI_UPDATEF_SUBFILE_MASK (DALI_UPDATEF_REPLACE_FILE | DALI_UPDATEF_CLONE_FROM | DALI_UPDATEF_APPEND_CLUSTER)
#define DALI_UPDATEF_MASK (DALI_UPDATEF_REPLACE_FILE | DALI_UPDATEF_CLONE_FROM | DALI_UPDATEF_APPEND_CLUSTER | DALI_UPDATEF_SUPERFILES | DALI_UPDATEF_PACKAGEMAP)

interface IDFUhelper: extends IInterface
{
    virtual void addSuper(const char *superfname, IUserDescriptor *user, unsigned numtoadd=0, const char **subfiles=NULL, const char *before=NULL, bool autocreatesuper = false) = 0;
    virtual void removeSuper(const char *superfname, IUserDescriptor *user, unsigned numtodelete=0, const char **subfiles=NULL, bool delsub=false, bool removesuperfile=true) = 0;
    virtual void listSubFiles(const char *superfname,StringAttrArray &out, IUserDescriptor *user) = 0;
    virtual StringBuffer &getFileXML(const char *lfn,StringBuffer &out, IUserDescriptor *user) = 0;
    virtual void addFileXML(const char *lfn,const StringBuffer &xml, const char * cluster, IUserDescriptor *user) = 0;
    virtual void addFileRemote(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser,IUserDescriptor *user) = 0;
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
                         const char *srcCluster,
                         const char *dstname,               // dst LFN
                         const char *cluster1,              // group name of roxie cluster
                         const char *prefix,
                         DFUclusterPartDiskMapping clustmap, // how the nodes are mapped
                         bool repeattlk,                    // repeat last part on all nodes if key
                         const char *cluster2,              // servers cluster (for just tlk)
                         IUserDescriptor *userdesc,         // user desc for local dali
                         const char *foreigndali,           // can be omitted if srcname foreign or local
                         IUserDescriptor *foreignuserdesc,  // user desc for foreign dali if needed
                         bool overwrite,                    // delete destination if exists
                         bool dophysicalcopy=false          // NB *not* using DFU server (so use with care)
                         ) = 0;


    virtual void cloneRoxieSubFile(const char *srcLFN,             // src LFN (can't be super)
                         const char *srcCluster,
                         const char *dstLFN,                       // dst LFN
                         const char *dstCluster,                   // group name of roxie cluster
                         const char *prefix,
                         unsigned redundancy,                      // Number of "spare" copies of the data
                         unsigned channelsPerNode,                 // Overloaded and cyclic modes
                         int replicateOffset,                      // Used In cyclic mode only
                         const char *defReplicateFolder,
                         IUserDescriptor *userdesc,                // user desc for local dali
                         const char *foreigndali,                  // can be omitted if srcname foreign or local
                         unsigned overwriteFlags                   // overwrite destination options
                         ) = 0;

    virtual void cloneFileRelationships(
        const char *foreigndali,        // where src relationships are retrieved from (can be NULL for local)
        StringArray &srcfns,            // file names on source
        StringArray &dstfns,            // corresponding filenames on dest (must exist otherwise ignored)
        IPropertyTree *relationships,   // if not NULL, tree will have all relationships filled in
        IUserDescriptor *user
    ) = 0;
};

IDFUhelper *createIDFUhelper();


#endif
