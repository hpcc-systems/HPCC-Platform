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

#ifndef TSortM_HPP
#define TSortM_HPP

#define ALWAYS_SORT_PRIMARY 0


interface ICompare;

interface ISortKeySerializer;
interface IRecordSize;
interface IRowInterfaces;

interface IThorSorterMaster: public IInterface
{
public:
    virtual int  AddSlave(ICommunicator *comm,rank_t rank, SocketEndpoint &endpoint,mptag_t mpTagRPC)=0;
    virtual void SortSetup(
                            IRowInterfaces *rowif,
                            ICompare * compare,
                            ISortKeySerializer *keyserializer, 
                            bool cosort,
                            bool needconnect,
                            const char *cosortfilenames,
                            IRowInterfaces *auxrowif
                        )=0;
    virtual void Sort(unsigned __int64 threshold, double skewWarning, double skewError, size32_t deviance, bool canoptimizenullcolumns, bool usepartitionrow, bool betweensort, unsigned minisortthresholdmb)=0;
    virtual bool MiniSort(rowcount_t totalrows)=0;
    virtual void SortDone()=0;
};


class CActivityBase;
IThorSorterMaster *CreateThorSorterMaster(CActivityBase *activity);



#endif
