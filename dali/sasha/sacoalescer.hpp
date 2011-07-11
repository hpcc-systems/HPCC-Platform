#ifndef SACOALESCER_HPP
#define SACOALESCER_HPP

interface ISashaServer;
extern ISashaServer *createSashaSDSCoalescingServer(); 
extern void suspendCoalescingServer();
extern void resumeCoalescingServer();

#endif
