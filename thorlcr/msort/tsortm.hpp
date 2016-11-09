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

#ifndef TSortM_HPP
#define TSortM_HPP

#define ALWAYS_SORT_PRIMARY 0

#ifdef THORSORT_EXPORTS
#define THORSORT_API DECL_EXPORT
#else
#define THORSORT_API DECL_IMPORT
#endif

interface ICompare;

interface ISortKeySerializer;
interface IRecordSize;
interface IThorRowInterfaces;

interface IThorSorterMaster: public IInterface
{
public:
    virtual int  AddSlave(ICommunicator *comm,rank_t rank, SocketEndpoint &endpoint,mptag_t mpTagRPC)=0;
    virtual void SortSetup(
                            IThorRowInterfaces *rowif,
                            ICompare * compare,
                            ISortKeySerializer *keyserializer, 
                            bool cosort,
                            bool needconnect,
                            const char *cosortfilenames,
                            IThorRowInterfaces *auxrowif
                        )=0;
    virtual void Sort(unsigned __int64 threshold, double skewWarning, double skewError, size32_t deviance, bool canoptimizenullcolumns, bool usepartitionrow, bool betweensort, unsigned minisortthresholdmb)=0;
    virtual bool MiniSort(rowcount_t totalrows)=0;
    virtual void SortDone()=0;
};


class CActivityBase;
THORSORT_API IThorSorterMaster *CreateThorSorterMaster(CActivityBase *activity);



#endif
