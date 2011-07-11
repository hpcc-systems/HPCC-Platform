/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

    function Stack()
    {
        // Create an empty array of cards.
        this.cards = new Array();  
        this.push  = StackPush; 
        this.pop   = StackPop;
        this.getData = StackGetData;
    }
 
    function StackPush(data)
    {
        this.cards.push(data);
    }
 
    function StackPop(data)
    {
        return this.cards.pop();            
    }
 
    function StackGetData()
    {
        return this.cards;
    }
