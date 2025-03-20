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

#include "platform.h"
#include "portlist.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jlzw.hpp"
#include "dafdesc.hpp"
#include "rmtclient.hpp"
#include "dautils.hpp"
#include "dasds.hpp"

#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "dameta.hpp"
#include "jsecrets.hpp"
#include "rmtfile.hpp"

#include <memory>

#define INCLUDE_1_OF_1    // whether to use 1_of_1 for single part files

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite

// These are legacy and cannot be changed.
#define SERIALIZATION_VERSION ((byte)0xd4)
#define SERIALIZATION_VERSION2 ((byte)0xd5) // with trailing superfile info

bool isMulti(const char *str)
{
    if (str&&!isSpecialPath(str))
        for (;;) {
            switch (*str) {
            case ',':
            case '*':
            case '?':
                return true;
            case 0:
                return false;
            }
            str++;
        }
    return false;
}


bool isCompressed(IPropertyTree &props, bool *blocked)
{
    if (props.getPropBool("@blockCompressed"))
    {
        if (blocked) *blocked = true;
        return true;
    }
    else
    {
        if (blocked) *blocked = false;
        return props.getPropBool("@rowCompressed");
    }
}

bool getCrcFromPartProps(IPropertyTree &fileattr,IPropertyTree &props, unsigned &crc)
{
    if (props.hasProp("@fileCrc"))
    {
        crc = (unsigned)props.getPropInt64("@fileCrc");
        return true;
    }
    // NB: old @crc keys and compressed were not crc of file but of data within.
    const char *kind = props.queryProp("@kind");
    if (strsame(kind,"key")) // key part
        return false;
    bool blocked;
    if (isCompressed(fileattr,&blocked)) {
        if (!blocked)
            return false;
        crc = COMPRESSEDFILECRC;
        return true;
    }
    if (!props.hasProp("@crc"))
        return false;
    crc = (unsigned)props.getPropInt64("@crc");
    return true;
}

void ClusterPartDiskMapSpec::setRoxie (unsigned redundancy, unsigned channelsPerNode, int _replicateOffset)
{
    flags = 0;
    replicateOffset = _replicateOffset?_replicateOffset:1;
#ifdef _CONTAINERIZED
    // NB: in containerized mode the cluster spec. redundancy isn't used.
    redundancy = 0;
#endif
    if ((channelsPerNode>1)&&(redundancy==0)) {
        flags |= CPDMSF_wrapToNextDrv;
        flags |= CPDMSF_overloadedConfig;
        maxDrvs = channelsPerNode;
    }
    else
        maxDrvs = (redundancy>1)?(redundancy+1):2;
    if (_replicateOffset==0)
        flags |= CPDMSF_fillWidth;
    startDrv = 0;
}

bool ClusterPartDiskMapSpec::calcPartLocation (unsigned part, unsigned maxparts, unsigned copy, unsigned clusterwidth, unsigned &node, unsigned &drv)
{
    // this is more cryptic than it could be (e.g. by special casing)
    // because it handles the cases that aren't going to ever happen, in a general way
    node = 0;
    drv = 0;
    if (!clusterwidth||!maxparts)
        return false;
    if (part>=maxparts)
        return false;

    unsigned nc = numCopies(part,clusterwidth,maxparts);
    if (copy>=nc)
        return false;
    unsigned dc=defaultCopies?defaultCopies:DFD_DefaultCopies;
    drv = startDrv;
    bool fw = (flags&CPDMSF_fillWidth)!=0;
    if (fw&&(maxparts>clusterwidth/2))
        fw = false;
    // calc primary
    node = part%clusterwidth;
    unsigned repdrv = startDrv+1;
    if (flags&CPDMSF_wrapToNextDrv) {
        drv += startDrv+(part/clusterwidth)%maxDrvs;
        repdrv = (1+(maxparts-1)/clusterwidth)%maxDrvs;
    }
    if (copy) {
        if (fw) {
            if (interleave>1)
                IERRLOG("ClusterPartDiskMapSpec interleave not allowed if fill width set");
            if (flags&CPDMSF_repeatedPart)
                IERRLOG("ClusterPartDiskMapSpec repeated part not allowed if fill width set");
            unsigned m = clusterwidth/maxparts;
            drv = startDrv+(repdrv+(copy/m-1))%maxDrvs;
            node += (copy%m)*maxparts;
        }
        else if ((flags&CPDMSF_repeatedPart)) {
            if (flags&CPDMSF_wrapToNextDrv)
                IERRLOG("ClusterPartDiskMapSpec repeated part not allowed if wrap to next drive set");
            unsigned repnum = copy%dc;
            unsigned nodenum = copy/dc;
            drv = startDrv+repnum%maxDrvs;
            if (interleave>1)
                node = (node+nodenum+(replicateOffset*repnum*interleave))%clusterwidth;
            else
                node = (node+nodenum+(replicateOffset*repnum))%clusterwidth;
        }
        else {
            drv = startDrv+(repdrv+copy-1)%maxDrvs;
            if (interleave>1)
                node = (node+(replicateOffset*copy*interleave))%clusterwidth;
            else
                node = (node+(replicateOffset*copy))%clusterwidth;
        }
    }
    return true;
}


inline void setPropDef(IPropertyTree *tree,const char *prop,int val,int def)
{
    if (val!=def)
        tree->setPropInt(prop,val);
    else
        tree->removeProp(prop);
}

inline int getPropDef(IPropertyTree *tree,const char *prop,int def)
{
    if (tree)
        return tree->getPropInt(prop,def);
    return def;
}

void ClusterPartDiskMapSpec::toProp(IPropertyTree *tree)
{
    if (!tree)
        return;
    setPropDef(tree,"@replicateOffset",replicateOffset,1);
    setPropDef(tree,"@redundancy",defaultCopies?(defaultCopies-1):1,1);
    setPropDef(tree,"@maxDrvs",maxDrvs?maxDrvs:2,2);
    setPropDef(tree,"@startDrv",startDrv,0);
    setPropDef(tree,"@interleave",interleave,0);
    setPropDef(tree,"@mapFlags",flags,0);
    setPropDef(tree,"@repeatedPart",repeatedPart,(int)CPDMSRP_notRepeated);
    setPropDef(tree, "@numStripedDevices", numStripedDevices, 1);
    if (defaultBaseDir.isEmpty())
        tree->removeProp("@defaultBaseDir");
    else
        tree->setProp("@defaultBaseDir",defaultBaseDir);
    if (defaultReplicateDir.isEmpty())
        tree->removeProp("@defaultReplicateDir");
    else
        tree->setProp("@defaultReplicateDir",defaultReplicateDir);
}

void ClusterPartDiskMapSpec::fromProp(IPropertyTree *tree)
{
    unsigned defrep = 1;
    // if directory is specified then must match default base to be default replicated
    StringBuffer dir;
    if (tree&&tree->getProp("@directory",dir)) {
        const char * base = queryBaseDirectory(grp_unknown, 0, SepCharBaseOs(getPathSepChar(dir.str())));
        size32_t l = strlen(base);
        if ((memcmp(base,dir.str(),l)!=0)||((l!=dir.length())&&!isPathSepChar(dir.charAt(l))))
            defrep = 0;
    }
    replicateOffset = getPropDef(tree,"@replicateOffset",1);
    defaultCopies = getPropDef(tree,"@redundancy",defrep)+1;
#ifdef _CONTAINERIZED
    // NB: in containerized mode the cluster spec. redundancy isn't used.
    // Force number of copies to 1 here (on deserialization) to avoid code paths that would look at redundant copies.
    defaultCopies = 1;
#endif
    maxDrvs = (byte)getPropDef(tree,"@maxDrvs",2);
    startDrv = (byte)getPropDef(tree,"@startDrv",defrep?0:getPathDrive(dir.str()));
    interleave = getPropDef(tree,"@interleave",0);
    flags = (byte)getPropDef(tree,"@mapFlags",0);
    repeatedPart = (unsigned)getPropDef(tree,"@repeatedPart",(int)CPDMSRP_notRepeated);
    numStripedDevices = (unsigned)getPropDef(tree, "@numStripedDevices", 1);
    setDefaultBaseDir(tree->queryProp("@defaultBaseDir"));
    setDefaultReplicateDir(tree->queryProp("@defaultReplicateDir"));
}

void ClusterPartDiskMapSpec::serialize(MemoryBuffer &mb)
{
    mb.append(flags);
    mb.append(replicateOffset);
    mb.append(defaultCopies);
    mb.append(startDrv);
    mb.append(maxDrvs);
    mb.append(interleave);
    if (flags&CPDMSF_repeatedPart)
        mb.append(repeatedPart);
    if (flags&CPDMSF_defaultBaseDir)
        mb.append(defaultBaseDir);
    if (flags&CPDMSF_defaultReplicateDir)
        mb.append(defaultReplicateDir);
    if (flags&CPDMSF_striped)
        mb.append(numStripedDevices);
}

void ClusterPartDiskMapSpec::deserialize(MemoryBuffer &mb)
{
    mb.read(flags);
    mb.read(replicateOffset);
    mb.read(defaultCopies);
#ifdef _CONTAINERIZED
    // NB: in containerized mode the cluster spec. redundancy isn't used.
    // Force number of copies to 1 here (on deserialization) to avoid code paths that would look at redundant copies.
    defaultCopies = 1;
#endif
    mb.read(startDrv);
    mb.read(maxDrvs);
    mb.read(interleave);
    if (flags&CPDMSF_repeatedPart)
        mb.read(repeatedPart);
    else
        repeatedPart = CPDMSRP_notRepeated;
    if (flags&CPDMSF_defaultBaseDir)
        mb.read(defaultBaseDir);
    else
        defaultBaseDir.clear();
    if (flags&CPDMSF_defaultReplicateDir)
        mb.read(defaultReplicateDir);
    else
        defaultReplicateDir.clear();
    if (flags&CPDMSF_striped)
        mb.read(numStripedDevices);
}


void ClusterPartDiskMapSpec::ensureReplicate()
{
    if (defaultCopies <= DFD_NoCopies)
        defaultCopies = DFD_DefaultCopies;
}

bool ClusterPartDiskMapSpec::isReplicated() const
{
    // If defaultCopies is zero (deprecated/legacy), the default value for replicated is true
    // Else, if it has any copy (>= 2), than it is replicated
    return defaultCopies != DFD_NoCopies;
}

unsigned ClusterPartDiskMapSpec::numCopies(unsigned part,unsigned clusterwidth, unsigned filewidth)
{
    if (flags&CPDMSF_repeatedPart) {
        if (repeatedPart&CPDMSRP_lastRepeated) {
            if (part+1==filewidth)
                return clusterwidth*defaultCopies;
        }
        else if ((part==(repeatedPart&CPDMSRP_partMask))||(repeatedPart&CPDMSRP_allRepeated))
            return clusterwidth*defaultCopies;
        if (repeatedPart&CPDMSRP_onlyRepeated)
            return 0;
    }
    return defaultCopies;
}

void ClusterPartDiskMapSpec::setRepeatedCopies(unsigned partnum,bool onlyrepeats)
{
    repeatedPart = partnum;
    if (partnum!=CPDMSRP_notRepeated) {
        flags |= CPDMSF_repeatedPart;
        if (onlyrepeats)
            repeatedPart |= CPDMSRP_onlyRepeated;
    }
    else
        flags &= ~CPDMSF_repeatedPart;
}

void ClusterPartDiskMapSpec::setDefaultBaseDir(const char *dir)
{
    defaultBaseDir.set(dir);
    if (defaultBaseDir.isEmpty())
        flags &= ~CPDMSF_defaultBaseDir;
    else
        flags |= CPDMSF_defaultBaseDir;
}

void ClusterPartDiskMapSpec::setDefaultReplicateDir(const char *dir)
{
    defaultReplicateDir.set(dir);
    if (defaultReplicateDir.isEmpty())
        flags &= ~CPDMSF_defaultReplicateDir;
    else
        flags |= CPDMSF_defaultReplicateDir;
}


ClusterPartDiskMapSpec & ClusterPartDiskMapSpec::operator=(const ClusterPartDiskMapSpec &other)
{
    replicateOffset = other.replicateOffset;
    defaultCopies = other.defaultCopies;
    maxDrvs = other.maxDrvs;
    startDrv = other.startDrv;
    flags = other.flags;
    interleave = other.interleave;
    repeatedPart = other.repeatedPart;
    setDefaultBaseDir(other.defaultBaseDir);
    setDefaultReplicateDir(other.defaultReplicateDir);
    numStripedDevices = other.numStripedDevices;
    return *this;
}


// --------------------------------------------------------



static void removeDir(const char *name,const char *dir,StringBuffer &out)
{
    const char *s=name;
    const char *d=dir;
    if (d&&*d) {
        while (*s&&(toupper(*s)==toupper(*d))) {
            s++;
            d++;
        }
        if ((*d==0)&&isPathSepChar(*s))  // support cross OS
            name = s+1;
    }
    out.append(name);
}

#define RO_SINGLE_PART (0x40000000) // used for singletons

struct CClusterInfo: implements IClusterInfo, public CInterface
{
    Linked<IGroup> group;
    StringAttr name; // group name
    ClusterPartDiskMapSpec mspec;
    bool foreignGroup = false;
    void checkClusterName(INamedGroupStore *resolver)
    {
        // check name matches group
        if (resolver&&group) {
            if (!name.isEmpty()) {
                StringBuffer defaultDir;
                GroupType groupType;
                Owned<IGroup> lgrp = resolver->lookup(name, defaultDir, groupType);
                if (lgrp&&lgrp->equals(group))
                {
                    if (mspec.defaultBaseDir.isEmpty())
                    {
                        mspec.setDefaultBaseDir(defaultDir);   // MORE - should possibly set up the rest of the mspec info from the group info here
                    }
                    if (mspec.defaultCopies>1 && mspec.defaultReplicateDir.isEmpty())
                    {
                        mspec.setDefaultReplicateDir(queryBaseDirectory(groupType, 1));  // MORE - not sure this is strictly correct
                    }
                    return; // ok
                }
                name.clear();
            }
            StringBuffer gname;
            if (resolver->find(group,gname,!foreignGroup)||(group->ordinality()>1))
                name.set(gname);
        }
    }
    void checkStriped()
    {
        if (!name.isEmpty())
        {
#ifdef _CONTAINERIZED
            Owned<IStoragePlane> plane = getDataStoragePlane(name, false);
            mspec.numStripedDevices = plane ? plane->numDevices() : 1;
            if (mspec.numStripedDevices>1)
                mspec.flags |= CPDMSF_striped;
            else
                mspec.flags &= ~CPDMSF_striped;
#else
            // Bare-metal can have multiple devices per plane (e.g. data + mirror), but it doesn't stripe across them
            mspec.numStripedDevices = 1;
#endif
        }
    }
public:
    IMPLEMENT_IINTERFACE;
    CClusterInfo(MemoryBuffer &mb,INamedGroupStore *resolver)
    {
        StringAttr grptext;
        mb.read(grptext);
        if (!grptext.isEmpty())
            group.setown(createIGroup(grptext));
        mspec.deserialize(mb);
        mb.read(name);
        checkClusterName(resolver);
    }

    CClusterInfo(const char *_name,IGroup *_group,const ClusterPartDiskMapSpec &_mspec,INamedGroupStore *resolver,unsigned flags)
        : group(_group), name(_name)
    {
        name.toLowerCase();
        mspec =_mspec;
        if (flags & IFDSF_FOREIGN_GROUP)
            foreignGroup = true;
        checkClusterName(resolver);
        checkStriped();
    }
    CClusterInfo(IPropertyTree *pt,INamedGroupStore *resolver,unsigned flags)
    {
        if (!pt)
            return;
        name.set(pt->queryProp("@name"));
        mspec.fromProp(pt);
        if (0 != (flags & IFDSF_FOREIGN_GROUP))
            foreignGroup = true;
        else if (pt->getPropBool("@foreign"))
            foreignGroup = true;

        if ((((flags&IFDSF_EXCLUDE_GROUPS)==0)||name.isEmpty())&&pt->hasProp("Group"))
            group.setown(createIGroup(pt->queryProp("Group")));
        if (!name.isEmpty()&&!group.get()&&resolver&&!foreignGroup)
        {
            StringBuffer defaultDir;
            GroupType groupType;
            group.setown(resolver->lookup(name.get(), defaultDir, groupType));
            // MORE - common some of this with checkClusterName?
            bool baseDirChanged = mspec.defaultBaseDir.isEmpty() || !strsame(mspec.defaultBaseDir, defaultDir);
            if (baseDirChanged)
                mspec.setDefaultBaseDir(defaultDir);   // MORE - should possibly set up the rest of the mspec info from the group info here

            if (mspec.defaultCopies>1 && (mspec.defaultReplicateDir.isEmpty() || baseDirChanged))
                mspec.setDefaultReplicateDir(queryBaseDirectory(groupType, 1));  // MORE - not sure this is strictly correct
        }
        else
            checkClusterName(resolver);
        checkStriped();
    }

    const char *queryGroupName()
    {
        return name.isEmpty()?NULL:name.get();
    }

    IGroup *queryGroup(IGroupResolver *resolver)
    {
        if (!group&&!name.isEmpty()&&resolver)
            group.setown(resolver->lookup(name));
        return group.get();
    }

    StringBuffer &getGroupName(StringBuffer &ret,IGroupResolver *resolver)
    {
        if (name.isEmpty()) {
            if (group)
            {
               if (resolver)
                    resolver->find(group,ret,true); // this will set single node as well
               else if (group->ordinality()==1)
                    group->getText(ret);
            }
        }
        else
            ret.append(name);
        return ret;
    }

    void serialize(MemoryBuffer &mb)
    {
        StringBuffer grptext;
        if (group)
            group->getText(grptext);
        mb.append(grptext);
        mspec.serialize(mb);
        mb.append(name);
    }
    INode *queryNode(unsigned idx,unsigned maxparts,unsigned copy)
    {
        if (!group.get())
            return queryNullNode();
        unsigned nn;
        unsigned dn;
        if (!mspec.calcPartLocation (idx,maxparts,copy, group->ordinality(), nn, dn))
            return queryNullNode();
        return &group->queryNode(nn);
    }
    unsigned queryDrive(unsigned idx,unsigned maxparts,unsigned copy)
    {
        if (!group.get())
            return 0;
        unsigned nn;
        unsigned dn;
        mspec.calcPartLocation (idx,maxparts,copy, group->ordinality(), nn, dn);
        return dn;
    }

    void serializeTree(IPropertyTree *pt,unsigned flags)
    {
        mspec.toProp(pt);
        if (group && (foreignGroup || ((flags&IFDSF_EXCLUDE_GROUPS)==0) || name.isEmpty()))
        {
            StringBuffer gs;
            group->getText(gs);
            pt->setProp("Group",gs.str());
            if (foreignGroup)
                pt->setPropBool("@foreign", true);
        }
        if (!name.isEmpty()&&((flags&IFDSF_EXCLUDE_CLUSTERNAMES)==0))
            pt->setProp("@name",name);
    }

    ClusterPartDiskMapSpec  &queryPartDiskMapping()
    {
        return mspec;
    }

    void setGroupName(const char *_name)
    {
        name.set(_name);
        name.toLowerCase();
    }

    void setGroup(IGroup *_group)
    {
        group.set(_group);
    }

    IGroup *queryGroup()
    {
        return group;
    }

    void getBaseDir(StringBuffer &basedir,DFD_OS os)
    {
        if (mspec.defaultBaseDir.isEmpty())  // assume current platform's default
            basedir.append(queryBaseDirectory(grp_unknown, 0, os));
        else
            basedir.append(mspec.defaultBaseDir);
    }

    void getReplicateDir(StringBuffer &basedir,DFD_OS os)
    {
        if (mspec.defaultReplicateDir.isEmpty())  // assume current platform's default
            basedir.append(queryBaseDirectory(grp_unknown, 1, os));
        else
            basedir.append(mspec.defaultReplicateDir);
    }

    StringBuffer &getClusterLabel(StringBuffer &ret)
    {
        return getGroupName(ret, NULL);
    }

    void applyPlane(IStoragePlane *plane)
    {
        mspec.numStripedDevices = plane ? plane->numDevices() : 1;
        if (mspec.numStripedDevices>1)
            mspec.flags |= CPDMSF_striped;
        else
            mspec.flags &= ~CPDMSF_striped;
    }
};

IClusterInfo *createClusterInfo(const char *name,
                                IGroup *grp,
                                const ClusterPartDiskMapSpec &mspec,
                                INamedGroupStore *resolver,
                                unsigned flags)
{
    return new CClusterInfo(name,grp,mspec,resolver,flags);
}
IClusterInfo *deserializeClusterInfo(MemoryBuffer &mb,
                                INamedGroupStore *resolver)
{
    return new CClusterInfo(mb,resolver);
}

IClusterInfo *deserializeClusterInfo(IPropertyTree *pt,
                                INamedGroupStore *resolver,
                                unsigned flags)
{
    return new CClusterInfo(pt,resolver,flags);
}


class CFileDescriptorBase: public CInterface
{
protected:
    PointerArray parts; // of CPartDescriptor

public:

    StringAttr tracename;
    unsigned lfnHash = 0;
    IArrayOf<IClusterInfo> clusters;

    Owned<IPropertyTree> attr;
    StringAttr directory;
    StringAttr partmask;
    FileDescriptorFlags fileFlags = FileDescriptorFlags::none;
    AccessMode accessMode = AccessMode::none;
    virtual unsigned numParts() = 0;                                            // number of parts
    virtual unsigned numCopies(unsigned partnum) = 0;                           // number of copies
    virtual INode *doQueryNode(unsigned partidx, unsigned copy, unsigned rn) = 0;               // query machine node
    virtual unsigned queryDrive(unsigned partidx, unsigned copy) = 0;           // query drive
    virtual StringBuffer &getPartTail(StringBuffer &name,unsigned idx) = 0;
    virtual StringBuffer &getPartDirectory(StringBuffer &name,unsigned idx,unsigned copy = 0) = 0;  // get filename dir
    virtual void serializePart(MemoryBuffer &mb,unsigned idx) = 0;
    virtual const char *queryDefaultDir() = 0;
    virtual IFileDescriptor &querySelf() = 0;
    virtual unsigned copyClusterNum(unsigned partidx, unsigned copy,unsigned *replicate=NULL) = 0;

    IPropertyTree & queryAttributes() const
    {
        return *attr;
    }
};

class CPartDescriptor : implements IPartDescriptor
{
protected: friend class CFileDescriptor;

    StringAttr overridename;    // this may be a multi path - may or not be relative to directory
                        // if not set use parent mask (and is *not* multi in this case)
    bool ismulti;       // only set if overridename set (otherwise false)
    CFileDescriptorBase &parent; // this is for the cluster *not* for the entire file

    unsigned partIndex;
    Owned<IPropertyTree> props;

public:

    virtual void Link(void) const
    {
        parent.Link();
    }
    virtual bool Release(void) const
    {
        return parent.Release();
    }


    CPartDescriptor(CFileDescriptorBase &_parent,unsigned idx,IPropertyTree *pt)
        : parent(_parent)
    {
        partIndex = idx;
        ismulti = false;
        if (pt && !isEmptyPTree(pt)) {
            if (pt->getPropInt("@num",idx+1)-1!=idx)
                IERRLOG("CPartDescriptor part index mismatch");
            overridename.set(pt->queryProp("@name"));
            if (overridename.isEmpty())
                overridename.clear();
            else
                ismulti = ::isMulti(overridename);
            props.setown(createPTreeFromIPT(pt));
            //props->removeProp("@num");        // keep these for legacy
            //props->removeProp("@name");
            props->removeProp("@node");
        }
        else
            props.setown(createPTree("Part"));
    }

    void set(unsigned idx, const char *_tail, IPropertyTree *pt)
    {
        partIndex = idx;
        setOverrideName(_tail);
        props.setown(pt?createPTreeFromIPT(pt):createPTree("Part"));
    }

    CPartDescriptor(CFileDescriptorBase &_parent, unsigned idx, MemoryBuffer &mb)
        : parent(_parent)
    {
        partIndex = idx;
        mb.read(overridename);
        if (overridename.isEmpty()) // shouldn't really need this
            overridename.clear();
        ismulti = ::isMulti(overridename);
        props.setown(createPTree(mb));
    }

    unsigned queryPartIndex()
    {
        return partIndex;
    }

    unsigned numCopies()
    {
        return parent.numCopies(partIndex);
    }

    virtual INode *queryNode(unsigned copy)
    {
        return parent.doQueryNode(partIndex,copy,(props&&props->hasProp("@rn"))?props->getPropInt("@rn"):(unsigned)-1);
    }

    virtual unsigned queryDrive(unsigned copy)
    {
        return parent.queryDrive(partIndex,copy);
    }

    INode *getNode(unsigned copy=0)
    {
        return LINK(queryNode(copy));
    }

    IPropertyTree &queryProperties()
    {
        return *props;
    }

    IPropertyTree *getProperties()
    {
        return props.get();
    }

    bool getCrc(unsigned &crc)
    {
        return getCrcFromPartProps(*parent.attr,*props,crc);
    }
    IFileDescriptor &queryOwner()
    {
        return parent.querySelf();
    }

    RemoteFilename &getFilename(unsigned copy, RemoteFilename &rfn)
    {
        if (ismulti) {
            RemoteMultiFilename rmfn;
            getMultiFilename(copy, rmfn);
            if (rmfn.ordinality()==1) {
                rfn.set(rmfn.item(0));
                return rfn;
            }
            throw MakeStringException(-1,"Remote Filename: Cannot resolve single part from wild/multi filename");
        }
        StringBuffer fullpath;
        getPath(fullpath,copy);
        rfn.setPath(queryNode(copy)->endpoint(),fullpath.str());
        return rfn;
    }

    StringBuffer &getPath(StringBuffer &path,unsigned copy)
    {
        StringBuffer tail;
        getTail(tail);
        if (!tail.length()||!isPathSepChar(tail.charAt(0))) {
            getDirectory(path,copy);
            addPathSepChar(path);
        }
        path.append(tail);
        return path;
    }

    StringBuffer &getTail(StringBuffer &name)
    {
        return parent.getPartTail(name,partIndex);
    }

    StringBuffer &getDirectory(StringBuffer &dir,unsigned copy)
    {
        return parent.getPartDirectory(dir,partIndex,copy);
    }

    bool isMulti()
    {
        return ismulti;
    }

    IPropertyTree & queryAttributes() const
    {
        return *props;
    }

    offset_t getFileSize(bool allowphysical,bool forcephysical)
    {
        offset_t ret = (offset_t)((forcephysical&&allowphysical)?-1:queryAttributes().getPropInt64("@size", -1));
        if (allowphysical&&(ret==(offset_t)-1))
            ret = getSize(true);
        return ret;
    }

    offset_t getDiskSize(bool allowphysical,bool forcephysical)
    {
        if (!::isCompressed(parent.queryAttributes()))
            return getFileSize(allowphysical, forcephysical);

        if (forcephysical && allowphysical)
            return getSize(false); // i.e. only if force, because all compressed should have @compressedSize attribute

        // NB: compressSize is disk size
        return queryAttributes().getPropInt64("@compressedSize", -1);
    }

    offset_t getSize(bool checkCompressed)
    {
        offset_t ret = (offset_t)-1;
        StringBuffer firstname;
        bool compressed = ::isCompressed(parent.queryAttributes());
        unsigned nc=parent.numCopies(partIndex);
        for (unsigned copy=0;copy<nc;copy++)
        {
            RemoteFilename rfn;
            try
            {
                Owned<IFile> partfile = createIFile(getFilename(copy, rfn));
                if (checkCompressed && compressed)
                {
                    Owned<IFileIO> partFileIO = partfile->open(IFOread);
                    if (partFileIO)
                    {
                        Owned<ICompressedFileIO> compressedIO = createCompressedFileReader(partFileIO);
                        if (compressedIO)
                            ret = compressedIO->size();
                        else
                            throw makeStringExceptionV(DFSERR_PhysicalCompressedPartInvalid, "Compressed part is not in the valid format: %s", partfile->queryFilename());
                    }
                }
                else
                    ret = partfile->size();
                if (ret!=(offset_t)-1)
                    return ret;
            }
            catch (IException *e)
            {
                StringBuffer s("CDistributedFilePart::getSize ");
                rfn.getRemotePath(s);
                EXCLOG(e, s.str());
                e->Release();
            }
            if (copy==0)
                rfn.getRemotePath(firstname);
        }
        throw makeStringExceptionV(DFSERR_CannotFindPartFileSize, "Cannot find physical file size for %s", firstname.str());;
    }


    RemoteMultiFilename &getMultiFilename(unsigned copy, RemoteMultiFilename &rmfn)
    {
        if (ismulti) {
            rmfn.setEp(queryNode(copy)->endpoint());
            StringBuffer dir;
            parent.getPartDirectory(dir,partIndex,copy);
            StringBuffer tmp1;
            StringBuffer tmp2;
            splitDirMultiTail(overridename,tmp1,tmp2);
            rmfn.append(tmp2, dir);
        }
        else {
            RemoteFilename rfn;
            getFilename(copy,rfn);
            rmfn.append(rfn);
        }
        return rmfn;
    }

    void subserialize(MemoryBuffer &mb)
    {
        mb.append(overridename);
        props->serialize(mb);
    }

    bool subserializeTree(IPropertyTree *pt)
    {
        bool ret = false;
        if (props) {
            Owned<IAttributeIterator> attriter = props->getAttributes();
            ForEach(*attriter) {
                const char *an = attriter->queryName();
                if ((stricmp(an,"@num")!=0)&&(stricmp(an,"@name")!=0)) {
                    pt->setProp(an,attriter->queryValue());
                    ret = true;
                }
            }
            Owned<IPropertyTreeIterator> iter = props->getElements("*");
            ForEach(*iter) {
                ret = true;
                pt->addPropTree(iter->query().queryName(),createPTreeFromIPT(&iter->query()));
            }
        }
        if (!overridename.isEmpty()) {
            pt->setProp("@name",overridename);
            ret = true;
        }
        if (ret)
            pt->setPropInt("@num",partIndex+1);
        if ((partIndex==0)&&(parent.numParts()==1)) {  // more legacy
            SocketEndpoint ep = queryNode(0)->endpoint();
            StringBuffer tmp;
            if (!ep.isNull())
                pt->setProp("@node",ep.getEndpointHostText(tmp).str());
            if (overridename.isEmpty()&&!parent.partmask.isEmpty()) {
                expandMask(tmp.clear(), parent.partmask, 0, 1);
                pt->setProp("@name",tmp.str());
            }
        }
        return ret;
    }

    void setOverrideName(const char *_tail)
    {
        if (!_tail||!*_tail)
            overridename.clear();
        else
            overridename.set(_tail);
        ismulti = ::isMulti(_tail);
    }
    const char *queryOverrideName()
    {
        if (overridename.isEmpty())
            return NULL;
        return overridename;
    }


    void serialize(MemoryBuffer &mb)
    {
        parent.serializePart(mb,partIndex);
    }

    unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL)
    {
        return parent.copyClusterNum(partIndex,copy,replicate);
    }

    IReplicatedFile *getReplicatedFile()
    {
        IReplicatedFile *ret = createReplicatedFile();
        RemoteFilenameArray &copies = ret->queryCopies();

        unsigned nc = numCopies();
        for (unsigned copy=0;copy<nc;copy++) {
            RemoteFilename rfn;
            copies.append(getFilename(copy,rfn));
        }
        return ret;
    }

};




// --------------------------------------------------------

class CPartDescriptorArrayIterator : public CArrayIteratorOf<IPartDescriptor, IPartDescriptorIterator>
{
public:
    CPartDescriptorArrayIterator() : CArrayIteratorOf<IPartDescriptor, IPartDescriptorIterator>(array) { }
    CPartDescriptorArray array;
};

void getClusterInfo(IPropertyTree &pt, INamedGroupStore *resolver, unsigned flags, IArrayOf<IClusterInfo> &clusters)
{
    unsigned nc = pt.getPropInt("@numclusters");
    if (!nc) { // legacy format
        unsigned np = pt.getPropInt("@numparts");
        StringArray groups;
        getFileGroups(&pt,groups);
        unsigned gi = 0;
        do {
            Owned<IGroup> cgroup;
            const char *grp = (gi<groups.ordinality())?groups.item(gi):NULL;
            if (grp&&resolver)
                cgroup.setown(resolver->lookup(grp));
            // get nodes from parts if complete (and group 0)
            if (gi==0) { // don't assume lookup name correct!
                MemoryBuffer mb;
                Owned<IPropertyTreeIterator> piter;
                if (pt.getPropBin("Parts",mb))
                    piter.setown(deserializePartAttrIterator(mb));
                else
                    piter.setown(pt.getElements("Part"));

                ForEach(*piter) {
                    IPropertyTree &cpt = piter->query();
                    unsigned num = cpt.getPropInt("@num");
                    if (num>np)
                        np = num;
                }

                std::unique_ptr<SocketEndpoint[]> eps(new SocketEndpoint[np?np:1]);
                ForEach(*piter) {
                    IPropertyTree &cpt = piter->query();
                    unsigned num = cpt.getPropInt("@num");
                    const char *node = cpt.queryProp("@node");
                    if (node&&*node)
                        eps.get()[num-1].set(node);
                }
                unsigned i=0;
                for (i=0;i<np;i++)
                    if (eps[i].isNull())
                        break;
                if (i==np) {
                    Owned<IGroup> ngrp = createIGroup(np,eps.get());
                    if (!cgroup.get()||(ngrp->compare(cgroup)!=GRbasesubset))
                        cgroup.setown(ngrp.getClear());
                }

            }
            ClusterPartDiskMapSpec mspec;
            IClusterInfo *cluster = createClusterInfo(grp,cgroup,mspec,resolver);
            clusters.append(*cluster);
            gi++;
        } while (gi<groups.ordinality());
    }
    else {
        Owned<IPropertyTreeIterator> iter = pt.getElements("Cluster");
        ForEach(*iter)
            clusters.append(*deserializeClusterInfo(&iter->query(),resolver,flags));
    }
}

inline const char *skipRoot(const char *lname)
{
    for (;;) {
        while (*lname==' ')
            lname++;
        if (*lname!='.')
            break;
        const char *s = lname+1;
        while (*s==' ')
            s++;
        if (!*s)
            lname = s;
        else if ((s[0]==':')&&(s[1]==':'))
            lname = s+2;
        else
            break;
    }
    return lname;
}

// returns position in result buffer of tail lfn name
static size32_t translateLFNToPath(StringBuffer &result, const char *lname, char pathSep)
{
    lname = skipRoot(lname);
    char c;
    size32_t l = result.length();
    while ((c=*(lname++))!=0)
    {
        if ((c==':')&&(*lname==':'))
        {
            lname++;
            result.clip().append(pathSep);
            l = result.length();
            lname = skipRoot(lname);
        }
        else if (validFNameChar(c))
            result.append((char)tolower(c));
        else
            result.appendf("%%%.2X", (int) c);
    }
    return l;
}

class CFileDescriptor:  public CFileDescriptorBase, implements ISuperFileDescriptor
{

    SocketEndpointArray *pending;   // for constructing cluster group
    Owned<IStoragePlane> remoteStoragePlane;
    std::vector<std::string> dafileSrvEndpoints;
    bool setupdone;
    byte version;

    IFileDescriptor &querySelf()
    {
        return *this;
    }


    void openPending()
    {
        if (!pending) {
            pending = new SocketEndpointArray;
            if (setupdone)
                throw MakeStringException(-1,"IFileDescriptor - setup already done");
            setupdone = true;
            ClusterPartDiskMapSpec mspec;
            clusters.append(*createClusterInfo(NULL,NULL,mspec));
        }


    }

    void doClosePending()
    {
        // first sort out cluster
        unsigned np = parts.ordinality();
        unsigned n = pending->ordinality();
        assertex(clusters.ordinality());
        assertex(np>=n);
        if (n==0) {
            clusters.remove(clusters.ordinality()-1);
            IWARNLOG("CFileDescriptor: removing empty cluster");
        }
        else {
            unsigned w;
            for (w=1;w<n;w++) {
                unsigned i;
                for (i=w;i<n;i++)
                    if (!pending->item(i).equals(pending->item(i%w)))
                        break;
                if (i==n)
                    break;

            }
            for (unsigned i=n;i>w;)
                pending->remove(--i);
            Owned<IGroup> newgrp = createIGroup(*pending);
            clusters.item(clusters.ordinality()-1).setGroup(newgrp);
        }
        delete pending;
        pending = NULL;
        if ((n==1)&&(isSpecialPath(part(0)->overridename)))
            return;
        // now look for a directory
        // this is a bit longwinded!
        // expand all tails
        StringBuffer tmp;
        if (!directory.isEmpty()) {
            StringBuffer fp;
            ForEachItemIn(i,parts) {

                CPartDescriptor *pt = part(i);
                if (!pt)
                    IWARNLOG("Null part in pending file descriptor");
                else if (pt->isMulti()) {
                    assertex(!pt->overridename.isEmpty());
                    if (!isAbsolutePath(pt->overridename)) {        // assumes all multi are same
                        mergeDirMultiTail(directory,pt->overridename,fp.clear()); // assumes all multi are same
                        pt->setOverrideName(fp.str());
                    }
                }
                else {
                    RemoteFilename rfn;
                    pt->getFilename(0,rfn);
                    rfn.getLocalPath(fp.clear());
                    pt->setOverrideName(fp.str());
                }
            }
        }
        directory.clear();
        StringBuffer dir;
        // now find longest common dir (multi complicates this somewhat)
        CPartDescriptor &part0 = *part(0);
        bool multi = part0.isMulti();
        if (multi)
            splitDirMultiTail(part0.overridename,dir,tmp);
        else
            splitDirTail(part0.overridename,dir);
        if (dir.length()==0) {
            IWARNLOG("CFileDescriptor cannot determine directory for file %s in '%s'",tracename.str(),part0.overridename.str());
        }
        else {
            const char *s = dir.str();
            for (unsigned i=1;i<np;i++) {
                CPartDescriptor &pt = *part(i);
                multi = pt.isMulti();
                StringBuffer tdir;      // would be easier without multi
                assertex(!pt.overridename.isEmpty());   // should have been set above
                if (multi) {
                    StringBuffer tmp;
                    splitDirMultiTail(pt.overridename,dir,tmp);
                }
                else
                    splitDirTail(part0.overridename,tdir);
                const char *t = tdir.str();
                const char *d = s;
                while (*d&&(*t==*d)) {
                    d++;
                    t++;
                }
                if (*t) { // not full match
                    while ((d!=s)&&!isPathSepChar(*(d-1)))
                        d--;
                    dir.setLength(d-s);
                    s = dir.str(); // paranoid
                    if (dir.length()<=2) // must be at least "/x/" or "d:\"
                        break; // no common dir
                }
            }
            if (dir.length()>2) {
                // now change all tails to relative
                StringBuffer relpath;
                for (unsigned i2=0;i2<np;i2++) {
                    CPartDescriptor &pt = *part(i2);
                    multi = pt.isMulti();
                    relpath.clear();
                    if (multi) {
                        removeRelativeMultiPath(pt.overridename,dir.str(),relpath);
                    }
                    else
                        relpath.append(splitRelativePath(pt.overridename,dir.str(),relpath));
                    pt.setOverrideName(relpath.str());
                }
                if ((dir.length()>1)&&(strcmp(dir.str()+1,":\\")!=0))
                    dir.setLength(dir.length()-1); // take off sep char
                directory.set(dir);
            }
        }
        // see if can create a partmask
        for (unsigned i=0;i<np;i++) {
            CPartDescriptor &pt = *part(i);
            if (pt.isMulti()) {
                partmask.clear();
                break;
            }
            if (!partmask.isEmpty()) {
                if (!matchesMask(pt.overridename,partmask,i,np)) {
                    partmask.clear();
                    if (i!=0)
                        break;
                }
            }
            if (partmask.isEmpty()&&!constructMask(partmask,pt.overridename,i,np))
                break;
        }
        if (partmask)
            for (unsigned i2=0;i2<np;i2++)
                part(i2)->setOverrideName(NULL);        // no longer need
    }

    inline void closePending()  // bit of a pain, but must be called at start of interrogation functions
    {
        if (pending)
            doClosePending();
    }


    StringBuffer &getPartTail(StringBuffer &name,unsigned idx)
    {
        unsigned n = numParts();
        if (idx<n) {
            CPartDescriptor &pt = *part(idx);
            if (!pt.overridename.isEmpty()) {
                if (isSpecialPath(pt.overridename))
                    return name.append(pt.overridename);
                if (pt.isMulti()) {
                    StringBuffer tmp;
                    splitDirMultiTail(pt.overridename,tmp,name);
                }
                else
                    name.append(pathTail(pt.overridename));
            }
            else if (!partmask.isEmpty())
                expandMask(name, pathTail(partmask), idx, n);
        }
        return name;
    }

    StringBuffer &getPartDirectory(StringBuffer &buf,unsigned idx,unsigned copy)
    {
        unsigned n = numParts();
        if (idx<n) {
            StringBuffer fullpath;
            StringBuffer tmp1;
            CPartDescriptor &pt = *part(idx);
            if (!pt.overridename.isEmpty()) {
                if (isSpecialPath(pt.overridename))
                    return buf;
                if (pt.isMulti()) {
                    StringBuffer tmpon; // bit messy but need to ensure dir put back on before removing!
                    const char *on = pt.overridename.get();
                    if (on&&*on&&!isAbsolutePath(on)&&!directory.isEmpty())
                        on = addPathSepChar(tmpon.append(directory)).append(on).str();
                    StringBuffer tmp2;
                    splitDirMultiTail(on,tmp1,tmp2);
                }
                else
                    splitDirTail(pt.overridename,tmp1);
                if (directory.isEmpty()||(isAbsolutePath(tmp1.str())||(stdIoHandle(tmp1.str())>=0)))
                    fullpath.swapWith(tmp1);
                else {
                    fullpath.append(directory);
                    if (fullpath.length())
                        addPathSepChar(fullpath);
                    fullpath.append(tmp1);
                }
            }
            else if (!partmask.isEmpty()) {
                fullpath.append(directory);
                if (containsPathSepChar(partmask)) {
                    if (fullpath.length())
                        addPathSepChar(fullpath);
                    splitDirTail(partmask,fullpath);
                }
            }
            else
                fullpath.append(directory);
            replaceClusterDir(idx,copy, fullpath);

            unsigned lcopy;
            IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
            if (cluster)
            {
                StringBuffer baseDir, repDir;
                DFD_OS os = SepCharBaseOs(getPathSepChar(fullpath));
                cluster->getBaseDir(baseDir, os);
                cluster->getReplicateDir(repDir, os);
                setReplicateFilename(fullpath,queryDrive(idx,copy),baseDir.str(),repDir.str());

                // The following code manipulates the directory for striping and aliasing if necessary.
                // To do so, it needs the plane details.
                // Normally, the plane name is obtained from IClusterInfo, however, if this file is foreign,
                // then the IClusterInfo's will have no resolved names (aka groups) because the remote groups
                // don't exist in the client environment. Instead, if the foreign file came from k8s, it will
                // have remoteStoragePlane serialized/set.
                Owned<IStoragePlane> plane;
                if (remoteStoragePlane)
                    plane.set(remoteStoragePlane);
                else
                {
                    const char *planeName = cluster->queryGroupName();
                    if (!isEmptyString(planeName))
                        plane.setown(getDataStoragePlane(planeName, false));
                }
                if (plane)
                {
                    StringBuffer planePrefix(plane->queryPrefix());
                    Owned<IStoragePlaneAlias> alias = plane->getAliasMatch(accessMode);
                    if (alias)
                    {
                        StringBuffer tmp;
                        StringBuffer newPlanePrefix(alias->queryPrefix());
                        if (setReplicateDir(fullpath, tmp, false, planePrefix, newPlanePrefix))
                        {
                            planePrefix.swapWith(newPlanePrefix);
                            fullpath.swapWith(tmp);
                        }
                    }
                    StringBuffer stripeDir;
                    addStripeDirectory(stripeDir, fullpath, planePrefix, idx, lfnHash, cluster->queryPartDiskMapping().numStripedDevices);
                    if (!stripeDir.isEmpty())
                        fullpath.swapWith(stripeDir);
                }
            }

            if ((fullpath.length()>3)&&isPathSepChar(fullpath.charAt(fullpath.length()-1)))
                fullpath.setLength(fullpath.length()-1);
            if (buf.length())
                buf.append(fullpath);
            else
                buf.swapWith(fullpath);
            if (FileDescriptorFlags::none != (fileFlags & FileDescriptorFlags::dirperpart))
                addPathSepChar(buf).append(idx+1); // part subdir 1 based
        }
        return buf;
    }


    IClusterInfo *queryCluster(unsigned partno,unsigned copy, unsigned &lcopy)
    {
        closePending();
        unsigned n=clusters.ordinality();
        unsigned i=0;
        bool c = 0;
        while (i<n) {
            unsigned mc = numClusterCopies(i,partno);
            if (copy<mc) {
                lcopy = copy;
                return &clusters.item(i);
            }
            copy -= mc;
            i++;
        }
        return NULL;
    }

    IClusterInfo *queryCluster(const char *_clusterName)
    {
        StringAttr clusterName = _clusterName;
        clusterName.toLowerCase();
        StringBuffer name;
        ForEachItemIn(c, clusters)
        {
            if (0 == strcmp(clusters.item(c).getClusterLabel(name.clear()).str(), clusterName))
                return &clusters.item(c);
        }
        return NULL;
    }

    virtual IClusterInfo *queryClusterNum(unsigned idx) override
    {
        return &clusters.item(idx);
    }

    void replaceClusterDir(unsigned partno,unsigned copy, StringBuffer &path)
    {
        // assumes default dir matches one of clusters
        closePending();
        if (path.length()<3)
            return;
        const char *ds = path.str();
        unsigned nc = clusters.ordinality();
        if (nc<=1)
            return; // not much can do
        StringAttr matched;
        StringAttr toadd;
        unsigned i=0;
        bool c = 0;
        int cp = (int)copy;
        while (i<nc) {
            StringBuffer dcmp;
            clusters.item(i).getBaseDir(dcmp,SepCharBaseOs(getPathSepChar(ds)));    // no trailing sep
            const char *t = dcmp.str();
            const char *d = ds;
            while (*d&&(*t==*d)) {
                d++;
                t++;
            }
            if (!*t&&(!*d||isPathSepChar(*d))&&(dcmp.length()>matched.length()))
                matched.set(dcmp);
            unsigned mc = numClusterCopies(i,partno);
            if ((cp>=0)&&(copy<mc))
                toadd.set(dcmp);
            copy -= mc;
            i++;
        }
        if (!matched.isEmpty()&&!toadd.isEmpty()&&(strcmp(matched,toadd)!=0)) {
            StringBuffer tmp(toadd);
            tmp.append(ds+matched.length());
            path.swapWith(tmp);
        }
    }

    // mapDafileSrvSecrets is a CFileDescriptor is created if it is associated with a remoteStoragePlane.
    // Identify the target dafilesrv location urls a secret based connections in the dafilesrv hook
    // NB: the expectation is that they'll only be 1 target service dafilesrv URL
    // These will remain associated in the hook, until this CFileDescriptor object is destroyed, and removeMappedDafileSrvSecrets is called.
    // 'optSecret' is supplied if an explicit secret was defined for the remote service (if blank, "<TLS>" is used as a placeholder by the caller)
    void mapDafileSrvSecrets(IClusterInfo &cluster, const char *optSecret)
    {
        Owned<INodeIterator> groupIter = cluster.queryGroup()->getIterator();

        ForEach(*groupIter)
        {
            INode &node = groupIter->query();
            StringBuffer endpointString;
            node.endpoint().getEndpointHostText(endpointString);
            auto it = std::find(dafileSrvEndpoints.begin(), dafileSrvEndpoints.end(), endpointString.str());
            if (it == dafileSrvEndpoints.end())
                dafileSrvEndpoints.push_back(endpointString.str());
        }
        for (auto &dafileSrvEp: dafileSrvEndpoints)
            queryDaFileSrvHook()->addSecretEndpoint(dafileSrvEp.c_str(), optSecret);
    }
    void removeMappedDafileSrvSecrets()
    {
        for (auto &dafileSrvEp: dafileSrvEndpoints)
            queryDaFileSrvHook()->removeSecretEndpoint(dafileSrvEp.c_str());
    }

    void mapSecrets()
    {
        if (attr->getPropBool("@_remoteSecure"))
        {
            const char *remoteSecret = attr->queryProp("@_remoteSecret");
            mapDafileSrvSecrets(clusters.item(0), remoteSecret);
        }
    }
public:
    IMPLEMENT_IINTERFACE;

    CFileDescriptor(MemoryBuffer &mb, IArrayOf<IPartDescriptor> *partsret, UnsignedArray **subcounts=NULL, bool *_interleaved=NULL)
    {
        // bit fiddly
        if (subcounts)
            *subcounts = NULL;
        pending = NULL;
        setupdone = true;
        mb.read(version);
        if ((version != SERIALIZATION_VERSION) && (version != SERIALIZATION_VERSION2)) // check serialization matched
            throw MakeStringException(-1,"FileDescriptor serialization version mismatch %d/%d",(int)SERIALIZATION_VERSION,(int)version);
        mb.read(tracename);
        mb.read(directory);
        mb.read(partmask);
        unsigned n;
        mb.read(n);
        for (unsigned i1 = 0; i1 < n; i1++)
            clusters.append(*deserializeClusterInfo(mb));
        unsigned partidx;
        mb.read(partidx);   // -1 if all parts, -2 if multiple parts
        mb.read(n); // numparts
        CPartDescriptor *part;
        if (partidx == (unsigned)-2)
        {
            UnsignedArray pia;
            unsigned pi;
            for (;;)
            {
                mb.read(pi);
                if (pi == (unsigned)-1)
                    break;
                pia.append(pi);
            }
            for (unsigned i3 = 0; i3 < n; i3++)
                parts.append(NULL);
            ForEachItemIn(i4, pia)
            {
                unsigned p = pia.item(i4);
                if (p < n) {
                    part = new CPartDescriptor(*this, p, mb);
                    parts.replace(part, p);
                }
            }
            if (partsret)
            {
                ForEachItemIn(i5, pia)
                {
                    unsigned p = pia.item(i5);
                    if (p < parts.ordinality())
                    {
                        CPartDescriptor *pt = (CPartDescriptor *)parts.item(p);
                        partsret->append(*LINK(pt));
                    }
                }
            }

        }
        else
        {
            for (unsigned i2=0; i2 < n; i2++)
            {
                if ((partidx == (unsigned)-1) || (partidx == i2))
                {
                    part = new CPartDescriptor(*this, i2, mb);
                    if (partsret)
                        partsret->append(*LINK(part));
                }
                else
                    part = NULL; // new CPartDescriptor(*this, i2, NULL);
                parts.append(part);
            }
        }
        attr.setown(createPTree(mb));
        if (attr)
        {
            lfnHash = attr->getPropInt("@lfnHash");
            // NB: for remote useDafilesrv use case
            IPropertyTree *remoteStoragePlaneMeta = attr->queryPropTree("_remoteStoragePlane");
            if (remoteStoragePlaneMeta)
            {
                assertex(1 == clusters.ordinality()); // only one cluster per logical remote file supported/will have resolved to 1
                remoteStoragePlane.setown(createStoragePlane(remoteStoragePlaneMeta));
                mapSecrets();
            }
        }
        else
            attr.setown(createPTree("Attr")); // doubt can happen
        fileFlags = static_cast<FileDescriptorFlags>(attr->getPropInt("@flags"));
        accessMode = static_cast<AccessMode>(attr->getPropInt("@accessMode"));
        if (version == SERIALIZATION_VERSION2)
        {
            if (subcounts)
                *subcounts = new UnsignedArray;
            unsigned n;
            mb.read(n);
            while (n)
            {
                unsigned np;
                mb.read(np);
                if (subcounts)
                    (*subcounts)->append(np);
                n--;
            }
            bool interleaved;
            mb.read(interleaved);
            if (_interleaved)
                *_interleaved = interleaved;
        }
    }

    void ensureRequiredStructuresExist()
    {
        if (!attr) attr.setown(createPTree("Attr"));
    }

    CFileDescriptor(IPropertyTree *tree, INamedGroupStore *resolver, unsigned flags)
    {
        pending = NULL;
        if ((flags&IFDSF_ATTR_ONLY)||!tree) {
            if (tree)
                attr.setown(tree);
            ensureRequiredStructuresExist();
            setupdone = false;
            return;
        }
        else
            setupdone = true;
        IPropertyTree &pt = *tree;
        tracename.set(pt.queryProp("@trace"));
        directory.set(pt.queryProp("@directory"));
        partmask.set(pt.queryProp("@partmask"));
        unsigned np = pt.getPropInt("@numparts");
        fileFlags = static_cast<FileDescriptorFlags>(pt.getPropInt("Attr/@flags"));
        accessMode = static_cast<AccessMode>(pt.getPropInt("Attr/@accessMode"));
        StringBuffer query;
        IPropertyTree **trees = NULL;
        Owned<IPropertyTreeIterator> piter;
        MemoryBuffer mb;
        IPropertyTree *at = pt.queryPropTree("Attr");
        getClusterInfo(pt,resolver,flags,clusters);
        offset_t totalsize = (offset_t)-1;
        if (flags&IFDSF_EXCLUDE_PARTS) {
            for (unsigned i2=0;i2<np;i2++)
                parts.append(new CPartDescriptor(*this,i2,NULL));
        }
        else {
            if (!at||(at->getPropInt64("@size",-1)==-1))
                totalsize = 0;
            if ((piter.get()&&mb.length())||pt.getPropBin("Parts",mb)) {
                if (!piter.get())
                    piter.setown(deserializePartAttrIterator(mb));
                unsigned i2=0;
                ForEach(*piter) {
                    if (totalsize!=(offset_t)-1) {
                        offset_t sz = piter->query().getPropInt64("@size",-1);
                        if (sz!=(offset_t)-1)
                            totalsize += sz;
                        else
                            totalsize = (offset_t)-1;
                    }
                    parts.append(new CPartDescriptor(*this,i2++,&piter->query()));
                }
            }
            else {  // parts may not be in order
                IArrayOf<IPropertyTree> trees;
                if (!piter.get())
                    piter.setown(pt.getElements("Part"));
                ForEach(*piter) {
                    IPropertyTree &cpt = piter->query();
                    unsigned num = cpt.getPropInt("@num");
                    if (!num)
                        continue;
                    while (num>trees.ordinality()+1)
                        trees.append(*createPTree("Part"));
                    cpt.Link();
                    if (num>trees.ordinality())
                        trees.append(cpt);
                    else
                        trees.replace(cpt,num-1);
                }
                for (unsigned i2=0;i2<np;i2++) {
                    if (totalsize!=(offset_t)-1) {
                        offset_t sz = (i2<trees.ordinality())?(offset_t)trees.item(i2).getPropInt64("@size",-1):(offset_t)-1;
                        if (sz!=(offset_t)-1)
                            totalsize += sz;
                        else
                            totalsize = (offset_t)-1;
                    }
                    parts.append(new CPartDescriptor(*this,i2,(i2<trees.ordinality())?&trees.item(i2):NULL));
                }
            }
        }
        piter.clear();
        if (at)
            attr.setown(createPTreeFromIPT(at));
        else
            attr.setown(createPTree("Attr"));
        if (flags & IFDSF_FOREIGN_GROUP)
            setFlags(static_cast<FileDescriptorFlags>(fileFlags | FileDescriptorFlags::foreign));
        if (attr->hasProp("@lfnHash")) // potentially missing for meta coming from a legacy Dali
            lfnHash = attr->getPropInt("@lfnHash");
        else if (tracename.length())
        {
            lfnHash = getFilenameHash(tracename.length(), tracename.str());
            attr->setPropInt("@lfnHash", lfnHash);
        }
        if (totalsize!=(offset_t)-1)
            attr->setPropInt64("@size",totalsize);

        // NB: for remote useDafilesrv use case
        IPropertyTree *remoteStoragePlaneMeta = at ? at->queryPropTree("_remoteStoragePlane") : nullptr;
        if (remoteStoragePlaneMeta)
        {
            assertex(1 == clusters.ordinality()); // only one cluster per logical remote file supported/will have resolved to 1
            remoteStoragePlane.setown(createStoragePlane(remoteStoragePlaneMeta));
            clusters.item(0).applyPlane(remoteStoragePlane);
            mapSecrets();
        }
    }

    void serializePart(MemoryBuffer &mb,unsigned partidx)
    {
        serializeParts(mb,&partidx,1);
    }

    void serializeParts(MemoryBuffer &mb,unsigned *partlist, unsigned nparts);

    void serializeParts(MemoryBuffer &mb,UnsignedArray &partlist)
    {
        serializeParts(mb,partlist.getArray(),partlist.ordinality());
    }

    void serialize(MemoryBuffer &mb)
    {
        serializePart(mb,(unsigned)-1);
    }

    void serializeTree(IPropertyTree &pt,unsigned flags)
    {
        closePending();
        if (!tracename.isEmpty())
            pt.setProp("@trace",tracename);
        if (!directory.isEmpty())
            pt.setProp("@directory",directory);
        if (!partmask.isEmpty())
            pt.setProp("@partmask",partmask);
        unsigned n = clusters.ordinality();
        pt.setPropInt("@numclusters",n);
        unsigned cn = 0;

// JCSMORE - afaics, IFileDescriptor @group is no longer used
        StringBuffer grplist;
        ForEachItemIn(i1,clusters) {
            Owned<IPropertyTree> ct = createPTree("Cluster");
            clusters.item(i1).serializeTree(ct,flags);
            if (!isEmptyPTree(ct)) {
                const char *cname = ct->queryProp("@name");
                if (cname&&*cname) {
                    if (grplist.length())
                        grplist.append(',');
                    grplist.append(cname);
                }
                pt.addPropTree("Cluster",ct.getClear());
            }
            else
                IWARNLOG("CFileDescriptor::serializeTree - empty cluster");
        }
        if (grplist.length())
            pt.setProp("@group",grplist.str());
        else
            pt.removeProp("@group");
/// ^^

        n = numParts();
        pt.setPropInt("@numparts",n);
        if ((flags&IFDSF_EXCLUDE_PARTS)==0) {
            if ((n==1)||((flags&CPDMSF_packParts)==0)) {
                for (unsigned i2=0;i2<n;i2++) {
                    Owned<IPropertyTree> p = createPTree("Part");
                    if (part(i2)->subserializeTree(p))
                        pt.addPropTree("Part",p.getClear());
                }
            }
            else {
                MemoryBuffer mb;
                for (unsigned i2=0;i2<n;i2++) {
                    // this seems a bit excessive in conversions
                    Owned<IPropertyTree> p = createPTree("Part");
                    part(i2)->subserializeTree(p);
                    serializePartAttr(mb,p);
                }
                pt.setPropBin("Parts",mb.length(),mb.toByteArray());
            }
        }
        IPropertyTree *t = &queryProperties();
        if (!isEmptyPTree(t))
            pt.addPropTree("Attr",createPTreeFromIPT(t));

    }

    IPropertyTree *getFileTree(unsigned flags)
    {
        Owned<IPropertyTree> ret = createPTree(queryDfsXmlBranchName(DXB_File));
        serializeTree(*ret,flags);
        return ret.getClear();
    }



    virtual ~CFileDescriptor()
    {
        removeMappedDafileSrvSecrets();
        closePending();             // not sure strictly needed
        ForEachItemInRev(p, parts)
            delpart(p);
    }

    void setDefaultDir(const char *dirname)
    {
        const char *s=dirname;
        size32_t l = strlen(s);
        char sc = 0;
        if ((l>1)&&(isPathSepChar(dirname[l-1]))&&(strcmp(dirname+1,":\\")!=0)) {
            l--;
            sc = dirname[l];
        }
        if (l&&!isAbsolutePath(dirname)) { // support cross-OS
            // assume relative path on same OS
            if (!sc)
                sc = getPathSepChar(dirname);
            StringBuffer tmp;
            tmp.append(queryBaseDirectory(grp_unknown, 0, SepCharBaseOs(sc)));
            if (sc != tmp.charAt(tmp.length()-1))
                tmp.append(sc);
            tmp.append(s);
            directory.set(tmp.str());
        }
        else
            directory.set(s,l);
    }

    int getReplicateOffset(unsigned clusternum)
    {
        closePending();
        if (clusternum>=clusters.ordinality())
            return 1;
        return clusters.item(clusternum).queryPartDiskMapping().replicateOffset;
    }

    CPartDescriptor *part(unsigned idx)
    {
        CPartDescriptor *ret = (CPartDescriptor *)parts.item(idx);
        if (!ret) { // this is not normally expected!
            ret = new CPartDescriptor(*this,idx,NULL);
            parts.replace(ret,idx);
        }
        return ret;
    }

    void delpart(unsigned idx)
    {
        CPartDescriptor *p = (CPartDescriptor *)parts.item(idx);
        delete p;
        parts.remove(idx);
    }

    void doSetPart(unsigned idx, const SocketEndpoint &ep, const char *filename, IPropertyTree *pt)
    {
        // setPart from ep/node ignores port in ep
        openPending();
        while (parts.ordinality()<=idx) {
            SocketEndpoint nullep;
            parts.append(new CPartDescriptor(*this,idx,NULL));
            pending->append(nullep);
        }
        CPartDescriptor &p = *part(idx);
        p.set(idx,filename,pt);
        if (idx>=pending->ordinality())
            IERRLOG("IFileDescriptor setPart called after cluster finished");
        else {
            SocketEndpoint &pep = pending->element(idx);
            if (pep.isNull())
                pep=ep;
            else
                IERRLOG("IFileDescriptor setPart called twice for same part");
        }
    }

    void setPart(unsigned idx, INode *node, const char *filename, IPropertyTree *pt)
    {
        if (node)
            setPart(idx,node->endpoint(),filename,pt); // ignore port
    }


    void setPart(unsigned idx, const IpAddress &ip, const char *filename, IPropertyTree *pt)
    {
        SocketEndpoint ep(0,ip);
        doSetPart(idx,ep,filename,pt);
    }

    void setPart(unsigned idx, const RemoteFilename &name, IPropertyTree *pt)
    {
        StringBuffer localname;
        name.getLocalPath(localname);
        SocketEndpoint ep = name.queryEndpoint();
        doSetPart(idx,ep,localname.str(),pt);
    }

    void setTraceName(const char *trc, bool normalize)
    {
        if (normalize)
        {
            CDfsLogicalFileName logicalName;
            logicalName.setAllowWild(true); // for cases where IFileDescriptor used to point to external files (e.g. during spraying)
            logicalName.set(trc); // normalize
            tracename.set(logicalName.get());
        }
        else
            tracename.set(trc);
        if (!queryProperties().hasProp("@lfnHash"))
        {
            lfnHash = getFilenameHash(tracename.length(), tracename.str());
            queryProperties().setPropInt("@lfnHash", lfnHash);
        }
    }

    unsigned numClusterCopies(unsigned cnum,unsigned partnum)
    {
        IClusterInfo &cluster = clusters.item(cnum);
        IGroup *grp = cluster.queryGroup();
        return cluster.queryPartDiskMapping().numCopies(partnum,grp?grp->ordinality():1,numParts());

    }

    unsigned numCopies(unsigned partnum)
    {
        closePending();
        unsigned ret = 0;
        ForEachItemIn(i,clusters)
            ret += numClusterCopies(i,partnum);
        return ret;
    }


    INode *getNode(unsigned partidx,unsigned copy)
    {
        INode *ret = queryNode(partidx,copy);
        return LINK(ret);
    }

    INode *doQueryNode(unsigned idx,unsigned copy,unsigned rn)
    {
        closePending();
        unsigned lcopy;
        IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
        if (!cluster)
            return queryNullNode();
        if ((copy==1)&&(rn!=(unsigned)-1)) {
            IGroup *group = cluster->queryGroup();
            if (group&&rn<group->ordinality())
                return &group->queryNode((rank_t)rn);
        }
        return cluster->queryNode(idx,numParts(),lcopy);
    }

    unsigned queryDrive(unsigned idx,unsigned copy)
    {
        closePending();
        unsigned lcopy;
        IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
        if (!cluster)
            return 0;
        return cluster->queryDrive(idx,numParts(),lcopy);
    }

    INode *queryNode(unsigned idx,unsigned copy)
    {
        closePending();
        if (idx<numParts())
            return part(idx)->queryNode(copy);
        return NULL;
    }


    RemoteFilename &getFilename(unsigned idx, unsigned copy, RemoteFilename &rfn)
    {
        closePending();
        return part(idx)->getFilename(copy, rfn);
    }


    StringBuffer &getTraceName(StringBuffer &str)
    {
        closePending();
        return str.append(tracename);
    }

    virtual IPropertyTree *getProperties()
    {
        closePending();
        return attr.getLink();
    }

    IPropertyTree &queryProperties()
    {
        closePending();
        return *attr.get();
    }

    IPropertyTree *queryHistory()
    {
        closePending();
        return attr->queryPropTree("History");
    }

    bool isMulti(unsigned partidx=(unsigned)-1)
    {
        closePending();
        if (partidx==(unsigned)-1) {
            for(partidx=0; partidx<numParts(); partidx++)
                if (part(partidx)->isMulti())
                    return true;
            return false;
        }
        return ((partidx<numParts()) && part(partidx)->isMulti());
    }

    RemoteMultiFilename &getMultiFilename(unsigned partidx,unsigned cpy, RemoteMultiFilename &rfn)
    {
        closePending();
        return part(partidx)->getMultiFilename(cpy, rfn);
    }


    IPartDescriptor *getPart(unsigned idx)
    {
        IPartDescriptor *ret = queryPart(idx);
        return LINK(ret);
    }

    IPartDescriptor *queryPart(unsigned idx)
    {
        closePending();
        if (idx<numParts())
            return part(idx);
        return NULL;
    }

    IPartDescriptorIterator *getIterator()
    {
        closePending();
        CPartDescriptorArrayIterator *iter = new CPartDescriptorArrayIterator();
        unsigned n=0;
        for (; n<numParts(); n++) iter->array.append(*getPart(n));
        return iter;
    }
    const char *queryKind()
    {
        return queryProperties().queryProp("@kind");
    }
    bool isGrouped()
    {
        return queryProperties().getPropBool("@grouped");
    }
    bool isCompressed(bool *blocked=NULL)
    {
        return ::isCompressed(queryProperties(), blocked);
    }


    const char *queryDefaultDir()
    {
        closePending();
        return directory;
    }

    void setPartMask(const char *mask)
    {
        partmask.set(mask);
    }

    unsigned addCluster(const char *name,IGroup *grp,const ClusterPartDiskMapSpec &map)
    {
        closePending();
        IClusterInfo * cluster = createClusterInfo(name,grp,map);
        clusters.append(*cluster);
        return clusters.ordinality()-1;
    }

    unsigned addCluster(IGroup *grp,const ClusterPartDiskMapSpec &map)
    {
        return addCluster(NULL,grp,map);
    }

    void endCluster(ClusterPartDiskMapSpec &map)
    {
        closePending();
        if (clusters.ordinality())
            clusters.item(clusters.ordinality()-1).queryPartDiskMapping() = map;
    }

    const char *queryPartMask()
    {
        closePending();
        return partmask;
    }

    IGroup *getGroup()
    {
        IGroup *g = queryClusterGroup(0);
        return LINK(g);
    }

    unsigned numParts()
    {
        closePending();
        return parts.ordinality();
    }

    void setNumParts(unsigned numparts)
    {
        closePending();
        while (parts.ordinality()<numparts)
            parts.append(new CPartDescriptor(*this,parts.ordinality(),NULL));
        while (parts.ordinality()>numparts)
            delpart(parts.ordinality()-1);
        if (::isMulti(partmask))
        {
            StringBuffer path;
            for (unsigned p=0; p<parts.ordinality(); p++)
            {
                CPartDescriptor *pt = (CPartDescriptor *)parts.item(p);
                pt->getPath(path.clear(), 0);
                pt->setOverrideName(path); // NB: override = 1st copy, getPartDirectory handles manipulating for other copies
            }
        }
    }

    unsigned numClusters()
    {
        closePending();
        return clusters.ordinality();
    }

    unsigned copyClusterNum(unsigned partidx, unsigned copy,unsigned *replicate=NULL)
    {
        unsigned lcopy=0;
        IClusterInfo * cluster = queryCluster(partidx,copy,lcopy);
        if (replicate)
            *replicate = lcopy;
        if (!cluster)
            return NotFound;
        // bit silly finding again
        return clusters.find(*cluster);
    }

    ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).queryPartDiskMapping();
    }

    IGroup *queryClusterGroup(unsigned clusternum)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).queryGroup();
    }


    void setClusterGroup(unsigned clusternum,IGroup *grp)
    {
        closePending();
        assertex(clusternum<numClusters());
        clusters.item(clusternum).setGroup(grp);
    }

    StringBuffer &getClusterGroupName(unsigned clusternum,StringBuffer &ret,IGroupResolver *resolver)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).getGroupName(ret,resolver);
    }

    void setClusterGroupName(unsigned clusternum,const char *name)
    {
        closePending();
        assertex(clusternum<numClusters());
        clusters.item(clusternum).setGroupName(name);
    }

    StringBuffer &getClusterLabel(unsigned clusternum,StringBuffer &ret)
        // either roxie label or node group name
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).getClusterLabel(ret);
    }

    void setClusterOrder(StringArray &names,bool exclusive)
    {
        closePending();
        unsigned done = 0;
        StringBuffer cname;
        ForEachItemIn(i,names)
        {
            StringAttr name = names.item(i);
            name.toLowerCase();
            for (unsigned j=done;j<clusters.ordinality();j++)
            {
                clusters.item(j).getClusterLabel(cname.clear());
                if (strcmp(cname.str(),name)==0)
                {
                    if (done!=j)
                        clusters.swap(done,j);
                    done++;
                    break;
                }
            }
        }
        if (exclusive)
        {
            if (!done)
                done = 1;
            StringAttr oldDefaultDir;
            StringBuffer baseDir1;
            while (clusters.ordinality()>done)
            {
                clusters.item(clusters.ordinality()-1).getBaseDir(baseDir1.clear(),SepCharBaseOs(getPathSepChar(directory)));

                // if baseDir is leading component this file's default directory..
                if (!oldDefaultDir.length() && directory.length()>=baseDir1.length() && 0==strncmp(directory, baseDir1, baseDir1.length()) &&
                    (directory.length()==baseDir1.length() || isPathSepChar(directory[baseDir1.length()])))
                    oldDefaultDir.set(baseDir1.str());
                clusters.remove(clusters.ordinality()-1);
            }
            if (oldDefaultDir.length() && clusters.ordinality())
            {
                StringBuffer baseDir2;
                clusters.item(0).getBaseDir(baseDir2.clear(), SepCharBaseOs(getPathSepChar(directory)));
                StringBuffer newDir(baseDir2.str());
                if (directory.length()>oldDefaultDir.length())
                    newDir.append(directory.get()+oldDefaultDir.length());
                directory.set(newDir.str());
            }
        }
    }

    virtual void ensureReplicate()
    {
        for (unsigned clusterIdx = 0; clusterIdx<numClusters(); clusterIdx++)
            queryPartDiskMapping(clusterIdx).ensureReplicate();
    }

    ISuperFileDescriptor *querySuperFileDescriptor()
    {
        return NULL;
    }

    bool mapSubPart(unsigned superpartnum, unsigned &subfile, unsigned &subpartnum)
    {
        // shouldn't get called ever
        subpartnum = superpartnum;
        subfile = 0;
        return true;
    }

    void setSubMapping(UnsignedArray &_subcounts, bool _interleaved)
    {
        UNIMPLEMENTED_X("setSubMapping called from CFileDescriptor!");
    }

    unsigned querySubFiles()
    {
        UNIMPLEMENTED_X("querySubFiles called from CFileDescriptor!");
    }

    virtual void setFlags(FileDescriptorFlags flags) override
    {
        queryProperties().setPropInt("@flags", static_cast<int>(flags));
        fileFlags = flags;
    }

    virtual FileDescriptorFlags getFlags() override
    {
        return static_cast<FileDescriptorFlags>(queryProperties().getPropInt("@flags"));
    }
};

class CSuperFileDescriptor:  public CFileDescriptor
{
    UnsignedArray *subfilecounts;
    bool interleaved;
public:

    CSuperFileDescriptor(MemoryBuffer &mb, IArrayOf<IPartDescriptor> *partsret)
        : CFileDescriptor(mb,partsret,&subfilecounts,&interleaved)
    {
    }

    CSuperFileDescriptor(IPropertyTree *attr)
        : CFileDescriptor(attr,NULL,IFDSF_ATTR_ONLY)    // only support attr here
    {
        subfilecounts = NULL;
    }

    virtual ~CSuperFileDescriptor()
    {
        delete subfilecounts;
    }

    ISuperFileDescriptor *querySuperFileDescriptor()
    {
        return this;
    }

    bool mapSubPart(unsigned superpartnum, unsigned &subfile, unsigned &subpartnum)
    {
        subpartnum = superpartnum;
        subfile = 0;
        if (!subfilecounts)  // its a file!
            return true;
        if (interleaved) {
            unsigned p = 0;
            unsigned f = 0;
            bool found = false;
            for (;;) {
                if (f==subfilecounts->ordinality()) {
                    if (!found)
                        break; // no more
                    found = false;
                    p++;
                    f = 0;
                }
                if (p<subfilecounts->item(f)) {
                    if (!superpartnum) {
                        subfile = f;
                        subpartnum = p;
                        return true;
                    }
                    superpartnum--;
                    found = true;
                }
                f++;
            }
        }
        else { // sequential
            while (subfile<subfilecounts->ordinality()) {
                if (subpartnum<subfilecounts->item(subfile))
                    return true;
                subpartnum -= subfilecounts->item(subfile);
                subfile++;
            }
        }
        return false;
    }

    void setSubMapping(UnsignedArray &_subcounts, bool _interleaved)
    {
        interleaved = _interleaved;
        if (_subcounts.ordinality()) {
            if (subfilecounts)
                subfilecounts->kill();
            else
                subfilecounts = new UnsignedArray;
            ForEachItemIn(i,_subcounts)
                subfilecounts->append(_subcounts.item(i));
        }
        else {
            delete subfilecounts;
            subfilecounts = NULL;
        }
    }

    unsigned querySubFiles()
    {
        if (!subfilecounts)  // its a file!
            return 1;
        return subfilecounts->ordinality();
    }

    void serializeSub(MemoryBuffer &mb)
    {
        if (subfilecounts) {
            unsigned count = subfilecounts->ordinality();
            mb.append(count);
            ForEachItemIn(i,*subfilecounts)
                mb.append(subfilecounts->item(i));
        }
        else
            mb.append((unsigned)0);
        mb.append(interleaved);
    }
};


void CFileDescriptor::serializeParts(MemoryBuffer &mb,unsigned *partlist, unsigned nparts)
{
    closePending();
    ISuperFileDescriptor *isdesc = querySuperFileDescriptor();
    CSuperFileDescriptor *sdesc = isdesc?(QUERYINTERFACE(isdesc,CSuperFileDescriptor)):NULL;
    mb.append(sdesc?SERIALIZATION_VERSION2:SERIALIZATION_VERSION);
    mb.append(tracename);
    mb.append(directory);
    mb.append(partmask);
    // first clusters
    unsigned n = clusters.ordinality();
    mb.append(n);
    ForEachItemIn(i1,clusters)
        clusters.item(i1).serialize(mb);
    n = numParts();
    if (nparts==1) {
        unsigned pi = *partlist;
        mb.append(pi).append(n);
        if (pi==(unsigned)-1) {
            for (unsigned i2=0;i2<n;i2++)
                part(i2)->subserialize(mb);
        }
        else if (pi<n)
            part(pi)->subserialize(mb);
    }
    else {
        mb.append((unsigned)-2).append(n);  // -2 is for multiple
        for (unsigned i3=0;i3<nparts;i3++)
            mb.append(partlist[i3]);
        mb.append((unsigned)-1); // end of list
        for (unsigned i4=0;i4<nparts;i4++)
            part(partlist[i4])->subserialize(mb);
    }
    queryProperties().serialize(mb);
    if (sdesc)
        sdesc->serializeSub(mb);
}


IFileDescriptor *createFileDescriptor(IPropertyTree *tree)
{
    return new CFileDescriptor(tree,NULL,IFDSF_ATTR_ONLY);
}

ISuperFileDescriptor *createSuperFileDescriptor(IPropertyTree *tree)
{
    return new CSuperFileDescriptor(tree);
}


IFileDescriptor *createFileDescriptor()
{
    return new CFileDescriptor(NULL,NULL,0);
}

static IFileDescriptor *_createExternalFileDescriptor(const char *_logicalname, bool lookup)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    //authentication already done
    SocketEndpoint ep;
    Owned<IGroup> group;
    if (!logicalname.getEp(ep))
    {
        StringBuffer grp;
        if (logicalname.getGroupName(grp).length()==0)
            throw MakeStringException(-1,"missing node in external file name (%s)",logicalname.get());
        group.setown(queryNamedGroupStore().lookup(grp.str()));
        if (!group)
            throw MakeStringException(-1,"cannot resolve node %s in external file name (%s)",grp.str(),logicalname.get());
        ep = group->queryNode(0).endpoint();
    }

    bool iswin=false;
    bool usedafs;
    switch (getDaliServixOs(ep))
    {
    case DAFS_OSwindows:
        iswin = true;
        // fall through
    case DAFS_OSlinux:
    case DAFS_OSsolaris:
        usedafs = ep.port||!ep.isLocal();
        break;
    default:
#ifdef _WIN32
        iswin = true;
#else
        iswin = false;
#endif
        usedafs = false;
        break;
    }

    //rest is local path
    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    StringBuffer dir;
    StringBuffer tail;
    IException *e=NULL;
    if (!logicalname.getExternalPath(dir,tail,iswin,&e))
    {
        if (e)
            throw e;
        return NULL;
    }
    fileDesc->setDefaultDir(dir.str());
    unsigned n = group.get()?group->ordinality():1;
    StringBuffer partname;
    CDateTime modTime;
    StringBuffer modTimeStr;
    for (unsigned i=0;i<n;i++)
    {
        if (group.get())
            ep = group->queryNode(i).endpoint();
        partname.clear();
        partname.append(dir);
        const char *s = tail.str();
        bool isspecial = (*s=='>');
        if (isspecial)
            partname.append(s);
        else
        {
            while (*s)
            {
                if (memicmp(s,"$P$",3)==0)
                {
                    partname.append(i+1);
                    s += 3;
                }
                else if (memicmp(s,"$N$",3)==0)
                {
                    partname.append(n);
                    s += 3;
                }
                else
                    partname.append(*(s++));
            }
        }
        if (!ep.port&&usedafs)
            ep.port = getDaliServixPort();
        RemoteFilename rfn;
        rfn.setPath(ep,partname.str());
        if (!isspecial&&(memcmp(partname.str(),"/$/",3)!=0)&&(memcmp(partname.str(),"\\$\\",3)!=0)) // don't get date on external data
        {
            try
            {
                Owned<IFile> file = createIFile(rfn);
                CDateTime dt;
                if (file&&file->getTime(NULL,&dt,NULL))
                {
                    if ((0 == modTimeStr.length())||(dt.compareDate(modTime)>0))
                    {
                        modTime.set(dt);
                        modTime.getString(modTimeStr);
                    }
                }
            }
            catch (IException *e)
            {
                EXCLOG(e,"CDistributedFileDirectory::createExternal");
                e->Release();
            }
        }
        if (lookup)
        {
            OwnedIFile iFile = createIFile(rfn);
            if (!iFile->exists())
                return NULL; // >=1 part does not exist.
        }
        if (modTimeStr.length())
        {
            Owned<IPropertyTree> part = createPTree("Part");
            part->setProp("@modified", modTimeStr.str());
            fileDesc->setPart(i, rfn, part);
        }
        else
            fileDesc->setPart(i, rfn);
    }
    fileDesc->queryPartDiskMapping(0).defaultCopies = DFD_NoCopies;
    return fileDesc.getClear();
}

IFileDescriptor *createExternalFileDescriptor(const char *logicalname)
{
    return _createExternalFileDescriptor(logicalname, false);
}
IFileDescriptor *getExternalFileDescriptor(const char *logicalname)
{
    return _createExternalFileDescriptor(logicalname, true);
}

inline void moveProp(IPropertyTree *to,IPropertyTree *from,const char *name)
{
    const char *p = from->queryProp(name);
    if (p&&*p) {
        to->setProp(name,p);
        from->removeProp(name);
    }
}

static CFileDescriptor * doDeserializePartFileDescriptors(MemoryBuffer &mb,IArrayOf<IPartDescriptor> *parts)
{
    size32_t savepos = mb.getPos();
    byte version;
    mb.read(version);
    mb.reset(savepos);
    if (version==SERIALIZATION_VERSION2) // its super
        return new CSuperFileDescriptor(mb,parts);
    return new CFileDescriptor(mb,parts);
}


extern da_decl void deserializePartFileDescriptors(MemoryBuffer &mb,IArrayOf<IPartDescriptor> &parts)
{
    Owned<CFileDescriptor> parent = doDeserializePartFileDescriptors(mb,&parts);
}


IPartDescriptor *deserializePartFileDescriptor(MemoryBuffer &mb)
{
    IArrayOf<IPartDescriptor> parts;
    Owned<CFileDescriptor> parent = doDeserializePartFileDescriptors(mb,&parts);
    if (parts.ordinality()!=1)
        IERRLOG("deserializePartFileDescriptor deserializing multiple parts not single part");
    if (parts.ordinality()==0)
        return NULL;
    return LINK(&parts.item(0));
}

IFileDescriptor *createFileDescriptor(const char *lname, const char *clusterType, const char *groupName, IGroup *group)
{
    StringBuffer partMask;
    unsigned parts = group->ordinality();
    getPartMask(partMask, lname, parts);

    StringBuffer curDir, defaultDir;
    if (!getConfigurationDirectory(nullptr, "data", clusterType, groupName, defaultDir))
        getLFNDirectoryUsingDefaultBaseDir(curDir, lname, DFD_OSdefault); // legacy
    else
        getLFNDirectoryUsingBaseDir(curDir, lname, defaultDir.str());

    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    fileDesc->setTraceName(lname);
    fileDesc->setNumParts(parts);
    fileDesc->setPartMask(partMask);
    fileDesc->setDefaultDir(curDir);

    ClusterPartDiskMapSpec mspec;
    mspec.defaultCopies = DFD_DefaultCopies;
    fileDesc->addCluster(groupName, group, mspec);

    return fileDesc.getClear();
}

IFileDescriptor *createFileDescriptor(const char *lname, const char *planeName, unsigned numParts)
{
    Owned<IStoragePlane> plane = getDataStoragePlane(planeName, true);
    if (!numParts)
        numParts = plane->numDefaultSprayParts();

    StringBuffer partMask, dir;
    getPartMask(partMask, lname, numParts);
    getLFNDirectoryUsingBaseDir(dir, lname, plane->queryPrefix());

    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    fileDesc->setTraceName(lname);
    fileDesc->setNumParts(numParts);
    fileDesc->setPartMask(partMask);
    fileDesc->setDefaultDir(dir);

    ClusterPartDiskMapSpec mspec;
    mspec.defaultCopies = DFD_NoCopies;
    Owned<IGroup> group = queryNamedGroupStore().lookup(planeName);
    fileDesc->addCluster(planeName, group, mspec);

    return fileDesc.getClear();
}

IFileDescriptor *deserializeFileDescriptor(MemoryBuffer &mb)
{
    return doDeserializePartFileDescriptors(mb,NULL);
}

IFileDescriptor *deserializeFileDescriptorTree(IPropertyTree *tree, INamedGroupStore *resolver, unsigned flags)
{
    return new CFileDescriptor(tree, resolver, flags);
}

static const char * defaultWindowsBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS] =
    {
            { "c:\\thordata", "d:\\thordata" },
            { "c:\\thordata", "d:\\thordata" },
            { "c:\\roxiedata", "d:\\roxiedata" },
            { "c:\\hthordata", "d:\\hthordata" },
            { "c:\\hthordata", "d:\\hthordata" },
    };
static const char * defaultUnixBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS] =
    {
#ifdef _CONTAINERIZED
        { "/var/lib/HPCCSystems/hpcc-data", "/var/lib/HPCCSystems/hpcc-mirror" },
        { "/var/lib/HPCCSystems/hpcc-data", "/var/lib/HPCCSystems/hpcc-mirror" },
        { "/var/lib/HPCCSystems/hpcc-data", "/var/lib/HPCCSystems/hpcc-data2", "/var/lib/HPCCSystems/hpcc-data3", "/var/lib/HPCCSystems/hpcc-data4" },
        { "/var/lib/HPCCSystems/hpcc-data", "/var/lib/HPCCSystems/hpcc-mirror" },
        { "/var/lib/HPCCSystems/mydropzone", "/var/lib/HPCCSystems/mydropzone-mirror" }, // NB: this is not expected to be used
        { "/var/lib/HPCCSystems/hpcc-data", "/var/lib/HPCCSystems/hpcc-mirror" },
#else
        { "/var/lib/HPCCSystems/hpcc-data/thor", "/var/lib/HPCCSystems/hpcc-mirror/thor" },
        { "/var/lib/HPCCSystems/hpcc-data/thor", "/var/lib/HPCCSystems/hpcc-mirror/thor" },
        { "/var/lib/HPCCSystems/hpcc-data/roxie", "/var/lib/HPCCSystems/hpcc-data2/roxie", "/var/lib/HPCCSystems/hpcc-data3/roxie", "/var/lib/HPCCSystems/hpcc-data4/roxie" },
        { "/var/lib/HPCCSystems/hpcc-data/eclagent", "/var/lib/HPCCSystems/hpcc-mirror/eclagent" },
        { "/var/lib/HPCCSystems/mydropzone", "/var/lib/HPCCSystems/mydropzone-mirror" }, // NB: this is not expected to be used
        { "/var/lib/HPCCSystems/hpcc-data/unknown", "/var/lib/HPCCSystems/hpcc-mirror/unknown" },
#endif
    };
static const char *componentNames[__grp_size] =
    {
        "thor", "thor", "roxie", "eclagent", "unknown"
    };
static const char *dirTypeNames[MAX_REPLICATION_LEVELS] =
    {
        "data", "data2", "data3", "data4"
    };

static StringAttr windowsBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS];
static StringAttr unixBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS];

static StringAttr defaultpartmask("$L$._$P$_of_$N$");

static CriticalSection ldbCs;
static std::atomic<bool> ldbDone{false};
static void loadDefaultBases()
{
    if (ldbDone)
        return;

    CriticalBlock b(ldbCs);
    if (ldbDone)
        return;
    ldbDone = true;

    SessionId mysessid = myProcessSession();
    if (mysessid)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software/Directories", mysessid, RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (conn) {
            IPropertyTree* dirs = conn->queryRoot();
            for (unsigned groupType = 0; groupType < __grp_size; groupType++)
            {
                const char *component = componentNames[groupType];
                for (unsigned replicationLevel = 0; replicationLevel < MAX_REPLICATION_LEVELS; replicationLevel++)
                {
                    StringBuffer dirout;
                    const char *dirType = dirTypeNames[replicationLevel];
                    if (replicationLevel==1 && groupType!=grp_roxie)
                        dirType = "mirror";
                    if (getConfigurationDirectory(dirs, dirType, component,
                        "dummy",   // NB this is dummy value (but actually hopefully not used anyway)
                        dirout))
                       unixBaseDirectories[groupType][replicationLevel].set(dirout.str());
                }
            }
        }
    }
    for (unsigned groupType = 0; groupType < __grp_size; groupType++)
        for (unsigned replicationLevel = 0; replicationLevel < MAX_REPLICATION_LEVELS; replicationLevel++)
        {
            if (unixBaseDirectories[groupType][replicationLevel].isEmpty())
                unixBaseDirectories[groupType][replicationLevel].set(defaultUnixBaseDirectories[groupType][replicationLevel]);
            if (windowsBaseDirectories[groupType][replicationLevel].isEmpty())
                windowsBaseDirectories[groupType][replicationLevel].set(defaultWindowsBaseDirectories[groupType][replicationLevel]);
        }
}


const char *queryBaseDirectory(GroupType groupType, unsigned replicateLevel, DFD_OS os)
{
    if (os==DFD_OSdefault)
#ifdef _WIN32
        os = DFD_OSwindows;
#else
        os = DFD_OSunix;
#endif
    assertex(replicateLevel < MAX_REPLICATION_LEVELS);
    loadDefaultBases();
    switch (os)
    {
    case DFD_OSwindows:
        return windowsBaseDirectories[groupType][replicateLevel];
    case DFD_OSunix:
        return unixBaseDirectories[groupType][replicateLevel];
    }
    return NULL;
}

void setBaseDirectory(const char * dir, unsigned replicateLevel, DFD_OS os)
{
    // 2 possibilities
    // either its an absolute path
    // or use /c$/thordata and /d$/thordata
    if (os==DFD_OSdefault)
#ifdef _WIN32
        os = DFD_OSwindows;
#else
        os = DFD_OSunix;
#endif
    assertex(replicateLevel < MAX_REPLICATION_LEVELS);
    loadDefaultBases();
    StringBuffer out;
    if (!dir||!*dir||!isAbsolutePath(dir))
        throw MakeStringException(-1,"setBaseDirectory(%s) requires an absolute path",dir ? dir : "null");
    size32_t l = strlen(dir);
    if ((l>3)&&(isPathSepChar(dir[l-1])))
        l--;
    switch (os) {
    case DFD_OSwindows:
        windowsBaseDirectories[grp_unknown][replicateLevel].set(dir,l);
        break;
    case DFD_OSunix:
        unixBaseDirectories[grp_unknown][replicateLevel].set(dir,l);
        break;
    }
}


const char *queryPartMask()
{
    return defaultpartmask.get();
}

void setPartMask(const char * mask)
{
    defaultpartmask.set(mask);
}

StringBuffer &getPartMask(StringBuffer &ret,const char *lname,unsigned partmax)
{
    // ret is in *and* out
    StringAttr tmp;
    const char *m;
    if (!ret.length())
        m = queryPartMask();
    else {
        tmp.set(ret.str());
        m = tmp.get();
        ret.clear();
    }
    StringBuffer lns;
    if (lname) {
        bool maybequery = false;
        const char *lnamebase = lname;
        for (;;) {
            const char *e = strstr(lname,"::");
            if (!e)
                break;
            lname = e+2;
            if (*lname=='>')
                maybequery = true;
        }
        if (maybequery) {
            CDfsLogicalFileName lfn;
            lfn.set(lnamebase);
            if (lfn.isQuery()) {
                RemoteFilename rfn;
                lfn.getExternalFilename(rfn);
                StringBuffer path;
                rfn.getPath(path);
                // start at third separator
                const char *s = path.str();
                unsigned si = 0;
                while (*s&&(si!=3)) {
                    if (isPathSepChar(*s))
                        si++;
                    s++;
                }
                return ret.append(s);
            }
        }
        char c;
        const char *l = lname;
        while ((c=*(l++))!=0) {
            if (validFNameChar(c))
                lns.append(c);
            else
                lns.appendf("%%%.2X", (int) c);
        }
        lns.trim().toLowerCase();
    }
    else if (!partmax)
        return ret.append(m);
    char c;
    while ((c=*(m++))!=0) {
        if (c=='$') {
            char pc = toupper(m[0]);
            if (pc&&(m[1]=='$')) {
                switch (pc) {
                case 'L':
                    if (lname) {
                        ret.append(lns.str());
                        m+=2;
                        continue;
                    }
                case 'N':
                    if (partmax) {
                        ret.append(partmax);
                        m+=2;
                        continue;
                    }
                }
            }
        }
        ret.append(c);
    }
    return ret;
}

StringBuffer &makePhysicalPartName(const char *lname, unsigned partno, unsigned partmax, StringBuffer &result, unsigned replicateLevel, DFD_OS os,const char *diroverride,bool dirPerPart,unsigned stripeNum)
{
    assertex(lname);
    if (strstr(lname,"::>")) { // probably query
        CDfsLogicalFileName lfn;
        lfn.set(lname);
        if (lfn.isQuery()) {
            RemoteFilename rfn;
            lfn.getExternalFilename(rfn);
            StringBuffer path;
            rfn.getPath(path);
            // query start at third separator
            const char *s = path.str();
            const char *sb = s;
            unsigned si = 0;
            while (*s&&(si!=3)) {
                if (isPathSepChar(*s)) {
                    if (os!=DFD_OSdefault)
                        path.setCharAt(s-sb,OsSepChar(os));
                    si++;
                }
                s++;
            }
            if (partno==0)
                return result.append(s-sb,sb);
            return result.append(sb);
        }
    }

    if (diroverride&&*diroverride) {
        if (os==DFD_OSdefault)
            os = SepCharBaseOs(getPathSepChar(diroverride));
        result.append(diroverride);
    }
    else
        result.append(queryBaseDirectory(grp_unknown, replicateLevel, os));

    size32_t l = result.length();
    if ((l>3)&&(result.charAt(l-1)!=OsSepChar(os))) {
        result.append(OsSepChar(os));
        l++;
    }

    if (stripeNum)
        result.append('d').append(stripeNum).append(OsSepChar(os));

    l = translateLFNToPath(result, lname, OsSepChar(os));

    char c;
    if (partno==0) // just return directory (with trailing PATHSEP)
        result.setLength(l);
    else
    {
#ifndef INCLUDE_1_OF_1
        if (partmax>1)  // avoid 1_of_1
#endif
        {
            StringBuffer tail(result.str()+l);
            tail.trim();
            result.setLength(l);
            if (dirPerPart && (partmax>1))
                result.append(partno).append(OsSepChar(os));
            const char *m = queryPartMask();
            while ((c=*(m++))!=0) {
                if (c=='$') {
                    char pc = toupper(m[0]);
                    if (pc&&(m[1]=='$')) {
                        switch (pc) {
                        case 'P':
                            result.append(partno);
                            m+=2;
                            continue;
                        case 'N':
                            result.append(partmax);
                            m+=2;
                            continue;
                        case 'L':
                            result.append(tail);
                            m+=2;
                            continue;
                        }
                    }
                }
                result.append(c);
            }
        }
    }
    return result.clip();
}

StringBuffer &makePhysicalDirectory(StringBuffer &result, const char *lname, unsigned replicateLevel, DFD_OS os,const char *diroverride)
{
    return makePhysicalPartName(lname, 0, 0, result, replicateLevel, os, diroverride, false, 0);
}


StringBuffer &makeSinglePhysicalPartName(const char *lname, StringBuffer &result, bool allowospath, bool &wasdfs,const char *diroverride)
{
    wasdfs = !(allowospath&&(isAbsolutePath(lname)||(stdIoHandle(lname)>=0)));
    if (wasdfs)
        return makePhysicalPartName(lname, 1, 1, result, 0, DFD_OSdefault, diroverride, false, 0);
    return result.append(lname);
}

StringBuffer &getLFNDirectoryUsingBaseDir(StringBuffer &result, const char *lname, const char *baseDir)
{
    assertex(lname);
    if (isEmptyString(baseDir))
        baseDir = queryBaseDirectory(grp_unknown, 0, DFD_OSdefault);

    result.append(baseDir);
    char pathSep = getPathSepChar(baseDir);
    size32_t l = result.length();
    if ((l>3) && (pathSep != result.charAt(l-1)))
    {
        result.append(pathSep);
        l++;
    }

    l = translateLFNToPath(result, lname, pathSep);
    result.setLength(l);
    return result.clip();
}

StringBuffer &getLFNDirectoryUsingDefaultBaseDir(StringBuffer &result, const char *lname, DFD_OS os)
{
    const char *baseDir = queryBaseDirectory(grp_unknown, 0, os);
    return getLFNDirectoryUsingBaseDir(result, lname, baseDir);
}

bool setReplicateDir(const char *dir,StringBuffer &out,bool isrep,const char *baseDir,const char *repDir)
{
    // assumes that dir contains a separator (like base)
    if (!dir)
        return false;
    const char *sep=findPathSepChar(dir);
    if (!sep)
        return false;
    DFD_OS os = SepCharBaseOs(*sep);
    const char *d = baseDir?baseDir:queryBaseDirectory(grp_unknown, isrep ? 0 : 1,os);
    if (!d)
        return false;
    unsigned match = 0;
    unsigned count = 0;
    unsigned i;
    for (i=0;d[i]&&dir[i]&&(d[i]==dir[i]);i++)
    {
        if (isPathSepChar(dir[i]))
        {
            match = i;
            count++;
        }
    }
    const char *r = repDir?repDir:queryBaseDirectory(grp_unknown, isrep ? 1 : 0,os);
    if (d[i]==0)
    {
        if (dir[i]==0)
        {
            // dir was an exact match for the base directory, return replica directory in output
            out.append(r);
            return true;
        }
        else if (isPathSepChar(dir[i]))
        {
            // dir matched the prefix of the base directory and the remaining part leads with a path separator
            // replace the base directory with the replica directory in the output, and append the remaining part of dir
            out.append(r);
            if (isPathSepChar(out.charAt(out.length()-1))) // if r has trailing path separator, skip the leading one in dir
                i++;
            out.append(dir+i);
            return true;
        }
        else if (i>0 && isPathSepChar(d[i-1])) // implies dir[i-1] is also a pathsepchar
        {
            // dir matched the prefix of the base directory, including the trailing path separator
            // replace the base directory with the replica directory in the output, and append the remaining part of dir
            out.append(r);
            addPathSepChar(out); // NB: this is an ensure-has-trailing-path-separator
            out.append(dir+i); // NB: dir+i is beyond a path separator in dir
            return true;
        }
    }
    else if (count) // this is a bit of a kludge to handle roxie backup
    {
        const char *s = r;
        const char *b = s;
        while (s&&*s)
        {
            if (isPathSepChar(*s))
            {
                if (--count==0)
                {
                    out.append(s-b,b).append(dir+match);
                    return true;
                }
            }
            s++;
        }
    }
    return false;
}


IFileDescriptor *createMultiCopyFileDescriptor(IFileDescriptor *in,unsigned num)
{
    Owned<IFileDescriptor> out = createFileDescriptor(createPTreeFromIPT(&in->queryProperties()));
    IPropertyTree &t = out->queryProperties();
    __int64 rc = t.getPropInt64("@recordCount",-1);
    if (rc>0)
        t.setPropInt64("@recordCount",rc*num);
    __int64 sz = t.getPropInt64("@size",-1);
    if (sz>0)
        t.setPropInt64("@size",sz*num);
    Owned<IPartDescriptorIterator> iter=in->getIterator();
    unsigned n = 0;
    while (num--) {
        if (iter->first()) {
            do {
                IPartDescriptor &part = iter->query();
                RemoteFilename rfn;
                part.getFilename(0,rfn);
                out->setPart(n,rfn,&part.queryProperties());
                n++;
            } while (iter->next());
        }
    }
    return out.getClear();
}

void removePartFiles(IFileDescriptor *desc,IMultiException *mexcept)
{
    if (!desc)
        return;
    CriticalSection crit;
    class casyncfor: public CAsyncFor
    {
        CriticalSection &crit;
        IMultiException *mexcept;
        IFileDescriptor *parent;
    public:
        casyncfor(IFileDescriptor *_parent,IMultiException *_mexcept,CriticalSection &_crit)
            : crit(_crit)
        {
            parent = _parent;
            mexcept = _mexcept;
        }
        void Do(unsigned i)
        {
            CriticalBlock block(crit);
            unsigned nc = parent->numCopies(i);
            for (unsigned copy = 0; copy < nc; copy++) {
                RemoteFilename rfn;
                parent->getFilename(i,copy,rfn);
                Owned<IFile> partfile = createIFile(rfn);
                StringBuffer eps;
                try
                {
                    unsigned start = msTick();
                    CriticalUnblock unblock(crit);
                    if (partfile->remove()) {
//                          PROGLOG("Removed '%s'",partfile->queryFilename());
                        unsigned t = msTick()-start;
                        if (t>60*1000)
                            OWARNLOG("Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getEndpointHostText(eps).str(), t/1000);
                    }
//                      else
//                          OWARNLOG("Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getEndpointHostText(eps).str());
                }
                catch (IException *e)
                {
                    if (mexcept)
                        mexcept->append(*e);
                    else {
                        StringBuffer s("Failed to remove file part ");
                        s.append(partfile->queryFilename()).append(" from ");
                        rfn.queryEndpoint().getEndpointHostText(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                }
            }
        }
    } afor(desc,mexcept,crit);
    afor.For(desc->numParts(),10,false,true);
}

StringBuffer &setReplicateFilename(StringBuffer &filename,unsigned drvnum,const char *baseDir,const char *repDir)
{
    if (!drvnum)
        return filename;  //do nothing!
    StringBuffer tmp(filename); // bit klunky
    if (strcmp(swapPathDrive(tmp,0,drvnum).str(),filename.str())!=0)
        tmp.swapWith(filename);
    else if (drvnum==1) { // OSS
        if(setReplicateDir(filename.str(),tmp.clear(),true,baseDir,repDir))
            tmp.swapWith(filename);
    }
    return filename;
}

IGroup *shrinkRepeatedGroup(IGroup *grp)
{
    if (!grp)
        return NULL;
    unsigned w = grp->ordinality();
    for (unsigned i=1;i<w;i++) {
        unsigned j;
        for (j=i;j<w;j++)
            if (!grp->queryNode(j).equals(&grp->queryNode(j%i)))
                break;
        if (j==w)
            return grp->subset(0U,i);
    }
    return LINK(grp);

}


IFileDescriptor *createFileDescriptorFromRoxieXML(IPropertyTree *tree,const char *clustername)
{
    if (!tree)
        return NULL;
    bool iskey = (strcmp(tree->queryName(),"Key")==0);
    Owned<IPropertyTree> attr = createPTree("Attr");
    Owned<IFileDescriptor> res = createFileDescriptor(attr.getLink());
    const char *id = tree->queryProp("@id");
    if (id) {
        if (*id=='~')
            id++;
        res->setTraceName(id);
    }
    else
        id = "";
    const char *dir = tree->queryProp("@directory");
    if (!dir||!*dir)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing directory",id);
    const char *mask = tree->queryProp("@partmask");
    if (!mask||!*mask)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part mask",id);
    unsigned np = tree->getPropInt("@numparts");
    IPropertyTree *part1 = tree->queryPropTree("Part_1");
    if (!part1)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1",id);

    // assume same number of copies for all parts
    unsigned nc = 0;
    StringBuffer xpath;
    StringBuffer locpath;
    StringArray locdirs;
    for (;;) {
        IPropertyTree *loc =  part1->queryPropTree(xpath.clear().appendf("Loc[%d]",nc+1));
        if (!loc)
            break;
        const char *path = loc->queryProp("@path");
        if (!path)
            throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1 loc path",id);
        RemoteFilename rfn;
        rfn.setRemotePath(path);
        if (rfn.queryEndpoint().isNull())
            break;
        locdirs.append(rfn.getLocalPath(locpath.clear()).str());
        nc++;
    }
    if (!nc)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1 Loc",id);
    StringBuffer fulldir(locdirs.item(0));
    addPathSepChar(fulldir).append(tree->queryProp("@directory"));
    res->setDefaultDir(fulldir.str());
    // create a group
    SocketEndpointArray *epa = new SocketEndpointArray[nc];
    for (unsigned p=1;p<=np;p++) {
        IPropertyTree *part = tree->queryPropTree(xpath.clear().appendf("Part_%d",p));
        if (!part)
            throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d",id,p);
        if (iskey&&(p==np)&&(np>1)) // leave off tlk
            continue;
        unsigned c;
        for(c = 0;c<nc;c++) {
            IPropertyTree *loc = part->queryPropTree(xpath.clear().appendf("Loc[%d]",c+1));
            if (loc) {
                const char *path = loc->queryProp("@path");
                if (!path)
                    throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d loc path",id,p);
                RemoteFilename rfn;
                rfn.setRemotePath(path);
                ForEachItemIn(d,locdirs) {
                    if (strcmp(rfn.getLocalPath(locpath.clear()).str(),locdirs.item(d))==0) {
                        SocketEndpoint ep = rfn.queryEndpoint();
                        if (ep.port==DAFILESRV_PORT || ep.port==SECURE_DAFILESRV_PORT)
                            ep.port = 0;
                        epa[d].append(ep);
                        break;
                    }
                }
            }
            else
                IERRLOG("createFileDescriptorFromRoxie: %s missing part %s",id,xpath.str());
        }
    }
    res->setPartMask(mask);
    res->setNumParts(np);
    SocketEndpointArray merged; // this is a bit odd but needed for when num parts smaller than cluster width
    ForEachItemIn(ei1,epa[0])
        merged.append(epa[0].item(ei1));
    for (unsigned enc=1;enc<nc;enc++) { // not quick! (n^2)
        ForEachItemIn(ei2,epa[enc]) {
            SocketEndpoint ep = epa[enc].item(ei2);
            ForEachItemIn(ei3,merged) {
                if (!merged.item(ei3).equals(ep)) {
                    merged.append(ep);
                    break;
                }
            }
        }
    }
    Owned<IGroup> epgrp = createIGroup(merged);
    Owned<IGroup> grp = shrinkRepeatedGroup(epgrp);
    // find replication offset
    ClusterPartDiskMapSpec map;
    if (nc) {
        map.replicateOffset = 0;
        unsigned i2;
        unsigned i3;
        for (;;) {
            for (i2=1;i2<nc;i2++) {
                for (i3=0;i3<epa[i2].ordinality();i3++) {
                    INode &node = grp->queryNode((i3+map.replicateOffset*i2)%grp->ordinality());
                    if (!node.endpoint().equals(epa[i2].item(i3)))
                        break;
                }
                if (i3<epa[i2].ordinality())
                    break;
            }
            if (i2==nc)
                break;
            map.replicateOffset++;
            if (map.replicateOffset==grp->ordinality())
                throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s cannot determine replication offset",id);
        }
    }

    map.defaultCopies = nc;
    if (iskey) {
        map.repeatedPart = (np-1);
        map.flags |= CPDMSF_repeatedPart;
    }
    if (clustername) {
#if 0
        Owned<IGroup> cgrp = queryNamedGroupStore().lookup(clustername);
        if (!cgrp)
            throw MakeStringException(-1,"createFileDescriptorFromRoxieXML: Cluster %s not found",clustername);
        if (!cgrp->equals(grp))
            throw MakeStringException(-1,"createFileDescriptorFromRoxieXML: Cluster %s does not match XML",clustername);
#endif
        res->addCluster(clustername,grp,map);
    }
    else
        res->addCluster(grp,map);
    delete [] epa;
    for (unsigned p=1;;p++) {
        IPropertyTree *part = tree->queryPropTree(xpath.clear().appendf("Part_%d",p));
        if (!part)
            break;
        IPropertyTree &pprop = res->queryPart(p-1)->queryProperties();
        StringBuffer ps;
        if (part->getProp("@crc",ps.clear())&&ps.length())
            pprop.setProp("@fileCRC",ps.str());
        if (part->getProp("@size",ps.clear())&&ps.length())
            pprop.setProp("@size",ps.str());
        if (part->getProp("@modified",ps.clear())&&ps.length())
            pprop.setProp("@modified",ps.str());
        if (iskey&&(p==np-1))
            pprop.setProp("@kind","topLevelKey");

#ifdef _DEBUG // test parts match
        unsigned c;
        for(c = 0;c<nc;c++) {
            IPropertyTree *loc = part->queryPropTree(xpath.clear().appendf("Loc[%d]",c+1));
            if (loc) {
                const char *path = loc->queryProp("@path");
                if (!path)
                    throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d loc path",id,c+1);
                StringBuffer fullpath(path);
                addPathSepChar(fullpath).append(tree->queryProp("@directory"));
                expandMask(addPathSepChar(fullpath),mask,p-1,np);
                RemoteFilename rfn;
                rfn.setRemotePath(fullpath.str());
                if (!rfn.queryEndpoint().isNull()) {
                    unsigned c2;
                    for (c2=0;c2<res->numCopies(p-1);c2++) {
                        RemoteFilename rfn2;
                        res->getFilename(p-1,c2,rfn2);
                        if (rfn2.equals(rfn))
                            break;
                        StringBuffer tmp;
                        rfn2.getPath(tmp);
                        //PROGLOG("%s",tmp.str());
                    }
                    if (c2==res->numCopies(p-1)) {
                        res->numCopies(p-1);
                        PROGLOG("ERROR: createFileDescriptorFromRoxie [%d,%d] %s not found",p,c,fullpath.str());
                    }
                }
            }
        }
#endif
    }
    IPropertyTree &fprop = res->queryProperties();
    StringBuffer fps;
    if (tree->getProp("@crc",fps.clear())&&fps.length())
        fprop.setProp("@checkSum",fps.str());
    if (tree->getProp("@recordCount",fps.clear())&&fps.length())
        fprop.setProp("@recordCount",fps.str());
    if (tree->getProp("@size",fps.clear())&&fps.length())
        fprop.setProp("@size",fps.str());
    if (tree->getProp("@formatCrc",fps.clear())&&fps.length())
        fprop.setProp("@formatCrc",fps.str());
    MemoryBuffer mb;
    if (tree->getPropBin("_rtlType", mb))
        fprop.setPropBin("_rtlType", mb.length(), mb.toByteArray());
    if (tree->getPropBin("_record_layout", mb.clear()))  // Legacy info
        fprop.setPropBin("_record_layout", mb.length(), mb.toByteArray());
    if (iskey) {
        fprop.setProp("@kind","key");
    }
    return res.getLink();
}

void extractFilePartInfo(IPropertyTree &info, IFileDescriptor &file)
{
    IPropertyTree *fileInfoTree = info.setPropTree("FileInfo", createPTree());
    Owned<IPartDescriptorIterator> partIter = file.getIterator();
    StringBuffer path, host;
    ForEach(*partIter)
    {
        IPropertyTree *partTree = fileInfoTree->addPropTree("Part", createPTree());
        IPartDescriptor &part = partIter->query();
        unsigned numCopies = part.numCopies();
        for (unsigned copy=0; copy<numCopies; copy++)
        {
            RemoteFilename rfn;
            part.getFilename(copy, rfn);

            IPropertyTree *copyTree = partTree->addPropTree("Copy", createPTree());
            copyTree->setProp("@filePath", rfn.getLocalPath(path.clear()));
            copyTree->setProp("@host", rfn.queryEndpoint().getEndpointHostText(host.clear()));
        }
    }
}

static void setupContainerizedStorageLocations()
{
    try
    {
        Owned<IPropertyTree> storage = getGlobalConfigSP()->getPropTree("storage");
        if (storage)
        {
            IPropertyTree * defaults = storage->queryPropTree("default");
            if (defaults)
            {
                const char * dataPath = defaults->queryProp("@data");
                if (dataPath)
                    setBaseDirectory(dataPath, 0);
                const char * mirrorPath = defaults->queryProp("@mirror");
                if (mirrorPath)
                    setBaseDirectory(mirrorPath, 1);
            }
        }
        else
        {
#ifdef _CONTAINERIZED
            throwUnexpectedX("config does not specify storage");
#endif
        }
    }
    catch (IException * e)
    {
        //If the component has not called the configuration init (e.g. esp in bare metal) then ignore the failure
        EXCLOG(e);
        e->Release();
    }
}

struct GroupInformation : public CInterface
{
public:
    GroupInformation(const char * _name) : name(_name) {}

    unsigned ordinality() const { return hosts.ordinality(); }

    bool checkIsSubset(const GroupInformation & other); // save information if it is a subset of other
    void createStoragePlane(IPropertyTree * storage, unsigned copy) const;

public:
    StringBuffer name;
    StringBuffer dir;
    StringArray hosts;
    const GroupInformation * container = nullptr;
    unsigned containerOffset = 0;
    GroupType groupType = grp_unknown;
    unsigned dropZoneIndex = 0;
};

using GroupInfoArray = CIArrayOf<GroupInformation>;

bool GroupInformation::checkIsSubset(const GroupInformation & other)
{
    //Find the first ip that matches, and then check the next ips within the other list also match
    unsigned thisSize = hosts.ordinality();
    unsigned otherSize = other.hosts.ordinality();
    if (thisSize > otherSize)
    {
        if (dropZoneIndex)
            return false;
        throwUnexpected();
    }
    for (unsigned i=0; i <= otherSize-thisSize; i++)
    {
        bool match = true;
        for (unsigned j=0; j < thisSize; j++)
        {
            if (!strieq(hosts.item(j), other.hosts.item(i+j)))
            {
                match = false;
                break;
            }
        }

        if (match)
        {
            container = &other;
            containerOffset = i;
            return true;
        }
    }
    return false;
}

void GroupInformation::createStoragePlane(IPropertyTree * storage, unsigned copy) const
{
    StringBuffer mirrorname;
    const char * planeName = name;
    if (copy != 0)
        planeName = mirrorname.append(name).append("_mirror");

    // Check that storage plane does not already have a definition
    VStringBuffer xpath("planes[@name='%s']", planeName);
    IPropertyTree * plane = storage->queryPropTree(xpath);
    if (!plane)
    {
        plane = storage->addPropTree("planes");
        plane->setProp("@name", planeName);
        plane->setPropBool("@fromGroup", true);
    }

    //URL style drop zones don't generate a host entry, and will have a single device
    if (ordinality() != 0)
    {
        if (!plane->hasProp("@hostGroup"))
        {
            if (container)
            {
                const char * containerName = container->name;
                if (copy != 0)
                    containerName = mirrorname.clear().append(containerName).append("_mirror");
                //hosts will be expanded by normalizeHostGroups
                plane->setProp("@hostGroup", containerName);
            }
            else
            {
                //Host group has been created that matches the name of the storage plane
                plane->setProp("@hostGroup", planeName);
            }
        }

        if (!plane->hasProp("@numDevices"))
        {
            if (ordinality() > 1)
            {
                plane->setPropInt("@numDevices", ordinality());
                if (dropZoneIndex == 0)
                    plane->setPropInt("@defaultSprayParts", ordinality());
            }
        }
    }

    if (!plane->hasProp("@prefix"))
    {
        if (dir.length())
            plane->setProp("@prefix", dir);
        else
            plane->setProp("@prefix", queryBaseDirectory(groupType, copy));
    }

    if (!plane->hasProp("@category"))
    {
        const char * category = (dropZoneIndex != 0) ? "lz" : "data";
        plane->setProp("@category", category);
    }

    //MORE: If container is identical to this except for the name we could generate an information tag @alias
}

static void appendGroup(GroupInfoArray & groups, GroupInformation * group)
{
    if (!group->name)
    {
        group->Release();
        return;
    }

    //Check for a duplicate group name.  This should never happen, but make sure it is caught if it did.
    ForEachItemIn(i, groups)
    {
        if (strieq(groups.item(i).name, group->name))
        {
            DBGLOG("Unexpected duplicate group name %s", group->name.str());
            group->Release();
            return;
        }
    }

    groups.append(*group);
}


static int compareGroupSize(CInterface * const * _left, CInterface * const * _right)
{
    const GroupInformation * left = static_cast<const GroupInformation *>(*_left);
    const GroupInformation * right = static_cast<const GroupInformation *>(*_right);
    //Ensure drop zones come after non drop zones, and the drop zone order is preserved
    if (left->dropZoneIndex || right->dropZoneIndex)
        return (int)(left->dropZoneIndex - right->dropZoneIndex);

    int ret = (int) (right->hosts.ordinality() - left->hosts.ordinality());
    if (ret)
        return ret;
    //Ensure thor groups come before non thor groups - so that a mirror host group will always be created
    if (left->groupType == grp_thor)
        return -1;
    if (right->groupType == grp_thor)
        return +1;

    return stricmp(left->name, right->name);
}


static void generateHosts(IPropertyTree * storage, GroupInfoArray & groups)
{
    //This function is potentially O(N^2) in the number of groups, and O(n^2) in the number of nodes within those groups.
    //In practice, it is unlikely to take very long unless there were vast numbers of groups.
    ForEachItemIn(i, groups)
    {
        GroupInformation & cur = groups.item(i);
        if (cur.ordinality() == 0)
            continue;

        GroupInformation * container = nullptr;
        for (unsigned j=0; j < i; j++)
        {
            GroupInformation & prev = groups.item(j);
            if (cur.checkIsSubset(prev))
                break;
        }

        // No containing hostGroup found, so generate a new entry for this set of hosts
        if (!cur.container)
        {
            IPropertyTree * plane = storage->addPropTree("hostGroups");
            plane->setProp("@name", cur.name);
            StringBuffer host;
            ForEachItemIn(i, cur.hosts)
                addPTreeItem(plane, "hosts", cur.hosts.item(i));
        }
        else if (cur.containerOffset || cur.ordinality() != cur.container->ordinality())
        {
            IPropertyTree * plane = storage->addPropTree("hostGroups");
            plane->setProp("@name", cur.name);
            plane->setProp("@hostGroup", cur.container->name);
            if (cur.containerOffset)
                plane->setPropInt("@offset", cur.containerOffset);
            if (cur.ordinality() != cur.container->ordinality())
                plane->setPropInt("@count", cur.ordinality());
            cur.container = nullptr;
        }

        //If this is a thor group, create a host group for the replicas - same list of ips, but offset by 1
        //If container is not null then there is already a hostGroup for an identical set of ips.  There will
        //also be a corresponding mirror because thor groups are sorted first.
        if (cur.groupType == grp_thor && !cur.container)
        {
            VStringBuffer mirrorName("%s_mirror", cur.name.str());

            IPropertyTree * plane = storage->addPropTree("hostGroups");
            plane->setProp("@name", mirrorName);
            plane->setProp("@hostGroup", cur.name);
            plane->setPropInt("@delta", 1);
        }
    }
}

static CConfigUpdateHook configUpdateHook;
static std::atomic<unsigned> normalizeHostGroupUpdateCBId{(unsigned)-1};
static CriticalSection storageCS;
static void doInitializeStorageGroups(bool createPlanesFromGroups, IPropertyTree * newGlobalConfiguration)
{
    CriticalBlock block(storageCS);
    IPropertyTree * storage = newGlobalConfiguration->queryPropTree("storage");
    if (!storage)
        storage = newGlobalConfiguration->addPropTree("storage");

    if (!isContainerized() && createPlanesFromGroups)
    {
        // Remove old planes created from groups
        while (storage->removeProp("planes[@fromGroup='1']"));
        storage->removeProp("hostGroups"); // generateHosts will recreate host groups for all planes

        GroupInfoArray allGroups;
        unsigned numDropZones = 0;

        //Create information about the storage planes from the groups published in dali
        //Use the Groups section directly, rather than queryNamedGroupStore(), so that hostnames are preserved
        {
            Owned<IRemoteConnection> conn = querySDS().connect("/Groups", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
            Owned<IPropertyTreeIterator> groups = conn->queryRoot()->getElements("Group");
            ForEach(*groups)
            {
                IPropertyTree & group = groups->query();
                Owned<GroupInformation> next = new GroupInformation(group.queryProp("@name"));
                next->groupType = translateGroupType(group.queryProp("@kind"));
                if (grp_dropzone == next->groupType)
                {
                    next->dir.set(group.queryProp("@dir"));
                    next->dropZoneIndex = ++numDropZones;
                    const char *ip = group.queryProp("Node[1]/@ip");
                    if (ip && !strieq(ip, "localhost"))
                        next->hosts.append(ip);
                }
                else
                {
                    Owned<IPropertyTreeIterator> nodes = group.getElements("Node");
                    ForEach(*nodes)
                        next->hosts.append(nodes->query().queryProp("@ip"));
                }
                appendGroup(allGroups, next.getClear());
            }
        }

        //Sort into size order, to help spot groups that are subsets of other groups
        allGroups.sort(compareGroupSize);

        //Now generate a list of hosts, if one group is a subset of another then do not generate the hosts
        generateHosts(storage, allGroups);

        //Finally generate the storage plane information
        ForEachItemIn(i, allGroups)
        {
            const GroupInformation & cur = allGroups.item(i);
            cur.createStoragePlane(storage, 0);
            if (cur.groupType == grp_thor)
                cur.createStoragePlane(storage, 1);
        }

        //Uncomment the following to trace the values that been generated
        //printYAML(storage);
    }

    //Ensure that host groups that are defined in terms of other host groups are expanded out so they have an explicit list of hosts
    normalizeHostGroups(newGlobalConfiguration);

    //The following can be removed once the storage planes have better integration
    setupContainerizedStorageLocations();
}

/*
 * This function is used to:
 *
 * (a) create storage planes from bare-metal groups
 * (b) to expand out host groups that are defined in terms of other host groups
 * (c) to setup the base directory for the storage planes.  (GH->JCS is this threadsafe?)
 *
 * For most components this init function is called only once - the exception is roxie (called when it connects and disconnects from dali).
 * In thor it is important that the update of the global config does not clone the property trees
 * In containerized mode, the update function can be called whenever the config file changes (but it will not be creating planes from groups)
 * In bare-metal mode the update function may be called whenever the environment changes in dali.  (see initClientProcess)
 *
 * Because of the requirement that thor does not clone the property trees, thor cannot support any background update - via the environment in
 * bare-metal, or config files in containerized.  If it was to support it the code in the master would need to change, and updates would
 * need to be synchronized to the workers.  To support this requiremnt a threadSafe parameter is provided to avoid the normal clone.
 *
 * For roxie, which dynamically connects and disconnects from dali, there are different problems.  The code needs to retain whether or not roxie
 * is connected to dali - and only create planes from groups if it is connected.  There is another potential problem, which I don't think will
 * actually be hit:
 * Say roxie connects, updates planes from groups and disconnects.  If the update functions were called again those storage planes would be lost,
 * but in bare-metal only a change to the environment will trigger an update, and that update will not happen if roxie is not connected to dali.
 */

static bool savedConnectedToDali = false; // Store in a global so it can be used by the callback without having to reregister
static bool lastUpdateConnectedToDali = false;
void initializeStoragePlanes(bool connectedToDali, bool threadSafe)
{
    {
        //If the createPlanesFromGroups parameter is now true, and was previously false, force an update
        CriticalBlock block(storageCS);
        if (!lastUpdateConnectedToDali && connectedToDali)
            configUpdateHook.clear();
        savedConnectedToDali = connectedToDali;
    }

    auto updateFunc = [](IPropertyTree * newComponentConfiguration, IPropertyTree * newGlobalConfiguration)
    {
        bool connectedToDali = savedConnectedToDali;
        lastUpdateConnectedToDali = connectedToDali;
        PROGLOG("initializeStoragePlanes update");
        doInitializeStorageGroups(connectedToDali, newGlobalConfiguration);
    };

    configUpdateHook.installModifierOnce(updateFunc, threadSafe);
}

void disableStoragePlanesDaliUpdates()
{
    savedConnectedToDali = false;
}

bool getDefaultStoragePlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getDefaultPlane(ret, "@dataPlane", "data"))
        return true;

    throwUnexpectedX("Default data plane not specified"); // The default should always have been configured by the helm charts
}

bool getDefaultSpillPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@spillPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@spillPlane", ret))
        return true;
    else if (getDefaultPlane(ret, nullptr, "spill"))
        return true;

    throwUnexpectedX("Default spill plane not specified"); // The default should always have been configured by the helm charts
}

bool getDefaultIndexBuildStoragePlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@indexBuildPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@indexBuildPlane", ret))
        return true;
    else
        return getDefaultStoragePlane(ret);
}

bool getDefaultPersistPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@persistPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@persistPlane", ret))
        return true;
    else
        return getDefaultStoragePlane(ret);
}

bool getDefaultJobTempPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@jobTempPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@jobTempPlane", ret))
        return true;
    else
    {
        // NB: In hthor jobtemps are written to the spill plane and hence ephemeral storage by default
        // In Thor they are written to the default data storage plane by default.
        // This is because HThor doesn't need them persisted beyond the lifetime of the process, but Thor does.
        return getDefaultStoragePlane(ret);
    }
}

//---------------------------------------------------------------------------------------------------------------------

static bool isAccessible(const IPropertyTree * xml)
{
    //Unusual to have components specified, so short-cicuit the common case
    if (!xml->hasProp("components"))
        return true;

    const char * thisComponentName = queryComponentName();
    if (!thisComponentName)
        return false;

    Owned<IPropertyTreeIterator> component = xml->getElements("components");
    ForEach(*component)
    {
        if (strsame(component->query().queryProp(nullptr), thisComponentName))
            return true;
    }
    return false;
}

class CStoragePlaneAlias : public CInterfaceOf<IStoragePlaneAlias>
{
public:
    CStoragePlaneAlias(IPropertyTree *_xml) : xml(_xml)
    {
        Owned<IPropertyTreeIterator> modeIter = xml->getElements("mode");
        ForEach(*modeIter)
        {
            const char *modeStr = modeIter->query().queryProp(nullptr);
            modes |= getAccessModeFromString(modeStr);
        }
        accessible = ::isAccessible(xml);
    }
    virtual AccessMode queryModes() const override { return modes; }
    virtual const char *queryPrefix() const override { return xml->queryProp("@prefix"); }
    virtual bool isAccessible() const override { return accessible; }

private:
    Linked<IPropertyTree> xml;
    AccessMode modes = AccessMode::none;
    bool accessible = false;
};

class CStorageApiInfo : public CInterfaceOf<IStorageApiInfo>
{
public:
    CStorageApiInfo(IPropertyTree * _xml) : xml(_xml)
    {
        if (!xml) // shouldn't happen
            throw makeStringException(MSGAUD_programmer, -1, "Invalid call: CStorageApiInfo(nullptr)");
    }
    virtual const char * getStorageType() const override
    {
        return xml->queryProp("@type");
    }
    virtual const char * queryStorageApiAccount(unsigned stripeNumber) const override
    {
        const char *account = queryContainer(stripeNumber)->queryProp("@account");
        if (isEmptyString(account))
            account = xml->queryProp("@account");
        return account;
    }
    virtual const char * queryStorageContainerName(unsigned stripeNumber) const override
    {
        return queryContainer(stripeNumber)->queryProp("@name");
    }
    virtual StringBuffer & getSASToken(unsigned stripeNumber, StringBuffer & token) const override
    {
        const char * secretName = queryContainer(stripeNumber)->queryProp("@secret");
        if (isEmptyString(secretName))
        {
            secretName = xml->queryProp("@secret");
            if (isEmptyString(secretName))
                return token.clear();  // return empty string if no secret name is specified
        }
        getSecretValue(token, "storage", secretName, "token", false);
        return token.trimRight();
    }

private:
    IPropertyTree * queryContainer(unsigned stripeNumber) const
    {
        if (stripeNumber==0) // stripeNumber==0 when not striped -> use first item in 'containers' list
            stripeNumber++;
        VStringBuffer path("containers[%u]", stripeNumber);
        IPropertyTree *container = xml->queryPropTree(path.str());
        if (!container)
            throw makeStringExceptionV(-1, "No container provided: path %s", path.str());
        return container;
    }
    Owned<IPropertyTree> xml;
};

class CStoragePlaneInfo : public CInterfaceOf<IStoragePlane>
{
public:
    CStoragePlaneInfo(IPropertyTree * _xml) : xml(_xml)
    {
        Owned<IPropertyTreeIterator> srcAliases = xml->getElements("aliases");
        ForEach(*srcAliases)
            aliases.push_back(new CStoragePlaneAlias(&srcAliases->query()));
        StringArray planeHosts;
        getPlaneHosts(planeHosts, xml);
        ForEachItemIn(h, planeHosts)
            hosts.emplace_back(planeHosts.item(h));
    }

    virtual const char * queryPrefix() const override { return xml->queryProp("@prefix"); }
    virtual unsigned numDevices() const override { return xml->getPropInt("@numDevices", 1); }
    virtual const std::vector<std::string> &queryHosts() const override
    {
        return hosts;
    }
    virtual unsigned numDefaultSprayParts() const override { return xml->getPropInt("@defaultSprayParts", 1); }
    virtual bool queryDirPerPart() const override { return xml->getPropBool("@subDirPerFilePart", isContainerized()); } // default to dir. per part in containerized mode

    virtual IStoragePlaneAlias *getAliasMatch(AccessMode desiredModes) const override
    {
        if (AccessMode::none == desiredModes)
            return nullptr;
        // go through and return one with most mode matches (should there be any other weighting?)
        unsigned bestScore = 0;
        IStoragePlaneAlias *bestMatch = nullptr;
        for (const auto & alias : aliases)
        {
            // Some aliases are only mounted in a restricted set of components (to avoid limits on the number of connections)
            if (!alias->isAccessible())
                continue;

            AccessMode aliasModes = alias->queryModes();
            unsigned match = static_cast<unsigned>(aliasModes & desiredModes);
            unsigned score = 0;
            while (match)
            {
                score += match & 1;
                match >>= 1;
            }
            if (score > bestScore)
            {
                bestScore = score;
                bestMatch = alias;
            }
        }
        return LINK(bestMatch);
    }
    virtual IStorageApiInfo *getStorageApiInfo()
    {
        IPropertyTree *apiInfo = xml->getPropTree("storageapi");
        if (apiInfo)
            return new CStorageApiInfo(xml);
        return nullptr;
    }

    virtual bool isAccessible() const override
    {
        return ::isAccessible(xml);
    }

private:
    Linked<IPropertyTree> xml;
    std::vector<Owned<IStoragePlaneAlias>> aliases;
    std::vector<std::string> hosts;
};


//MORE: This could be cached
static IStoragePlane * getStoragePlane(const char * name, const std::vector<std::string> &categories, bool required)
{
    VStringBuffer xpath("storage/planes[@name='%s']", name);
    Owned<IPropertyTree> match = getGlobalConfigSP()->getPropTree(xpath);
    if (!match)
    {
        if (required)
            throw makeStringExceptionV(-1, "Unknown storage plane '%s'", name);
        return nullptr;
    }
    const char * category = match->queryProp("@category");
    auto r = std::find(categories.begin(), categories.end(), category);
    if (r == categories.end())
    {
        if (required)
            throw makeStringExceptionV(-1, "storage plane '%s' does not match request categories (plane category=%s)", name, category);
        return nullptr;
    }

    return new CStoragePlaneInfo(match);
}

IStoragePlane * getDataStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();

    // NB: need to include "remote" planes too, because std. file access will encounter
    // files on the "remote" planes, when they have been remapped to them via ~remote access
    return getStoragePlane(group, { "data", "lz", "remote" }, required);
}

IStoragePlane * getRemoteStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();
    return getStoragePlane(group, { "remote" }, required);
}

IStoragePlane * createStoragePlane(IPropertyTree *meta)
{
    return new CStoragePlaneInfo(meta);
}


AccessMode getAccessModeFromString(const char *access)
{
    // use a HT?
    if (streq(access, "read"))
        return AccessMode::read;
    else if (streq(access, "write"))
        return AccessMode::write;
    else if (streq(access, "random"))
        return AccessMode::random;
    else if (streq(access, "sequential"))
        return AccessMode::sequential;
    else if (streq(access, "noMount"))
        return AccessMode::noMount;
    else if (isEmptyString(access))
        return AccessMode::none;
    throwUnexpectedX("getAccessModeFromString : unrecognized access mode string");
}

unsigned __int64 getPartPlaneAttr(IPartDescriptor &part, unsigned copy, PlaneAttributeType attr, size32_t defaultValue)
{
    unsigned clusterNum = part.copyClusterNum(copy);
    StringBuffer planeName;
    part.queryOwner().getClusterLabel(clusterNum, planeName);
    return getPlaneAttributeValue(planeName, attr, defaultValue);
}

