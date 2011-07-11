/*******************************************************************************************
 * Object: Hashtable
 * Description: Implementation of hashtable
 * Author: Uzi Refaeli
 *******************************************************************************************/

//======================================= Properties ========================================
Hashtable.prototype.hash        = null;
Hashtable.prototype.keys        = null;
Hashtable.prototype.location    = null;

/**
 * Hashtable - Constructor
 * Create a new Hashtable object.
 */
function Hashtable(){
    this.hash = new Array();
    this.keys = new Array();

    this.location = 0;
}

/**
 * put
 * Add new key
 * param: key - String, key name
 * param: value - Object, the object to insert
 */
Hashtable.prototype.put = function (key, value){
    if (value == null)
        return;

    if (this.hash[key] == null)
        this.keys[this.keys.length] = key;

    this.hash[key] = value;
}

/**
 * get
 * Return an element
 * param: key - String, key name
 * Return: object - The requested object
 */
Hashtable.prototype.get = function (key){
        return this.hash[key];
}

/**
 * remove
 * Remove an element
 * param: key - String, key name
 */
Hashtable.prototype.remove = function (key){
    for (var i = 0; i < this.keys.length; i++){
        //did we found our key?
        if (key == this.keys[i]){
            //remove it from the hash
            this.hash[this.keys[i]] = null;
            //and throw away the key...
            this.keys.splice(i ,1);
            return;
        }
    }
}

/**
 * size
 * Return: Number of elements in the hashtable
 */
Hashtable.prototype.size = function (){
    return this.keys.length;
}

/**
 * populateItems
 * Deprecated
 */
Hashtable.prototype.populateItems = function (){}

/**
 * next
 * Return: true if theres more items
 */
Hashtable.prototype.next = function (){
    if (++this.location < this.keys.length)
        return true;
    else
        return false;
}

/**
 * moveFirst
 * Move to the first item.
 */
Hashtable.prototype.moveFirst = function (){
    try {
        this.location = -1;
    } catch(e) {/*//do nothing here :-)*/}
}

/**
 * moveLast
 * Move to the last item.
 */
Hashtable.prototype.moveLast = function (){
    try {
        this.location = this.keys.length - 1;
    } catch(e) {/*//do nothing here :-)*/}
}

/**
 * getKey
 * Return: The value of item in the hash
 */
Hashtable.prototype.getKey = function (){
    try {
        return this.keys[this.location];
    } catch(e) {
        return null;
    }
}

/**
 * getValue
 * Return: The value of item in the hash
 */
Hashtable.prototype.getValue = function (){
    try {
        return this.hash[this.keys[this.location]];
    } catch(e) {
        return null;
    }
}

/**
 * getKey
 * Return: The first key contains the given value, or null if not found
 */
Hashtable.prototype.getKeyOfValue = function (value){
    for (var i = 0; i < this.keys.length; i++)
        if (this.hash[this.keys[i]] == value)
            return this.keys[i]
    return null;
}


/**
 * toString
 * Returns a string representation of this Hashtable object in the form of a set of entries,
 * enclosed in braces and separated by the ASCII characters ", " (comma and space).
 * Each entry is rendered as the key, an equals sign =, and the associated element,
 * where the toString method is used to convert the key and element to strings.
 * Return: a string representation of this hashtable.
 */
Hashtable.prototype.toString = function (){

    try {
        var s = new Array(this.keys.length);
        s[s.length] = "{";

        for (var i = 0; i < this.keys.length; i++){
            s[s.length] = this.keys[i];
            s[s.length] = "=";
            var v = this.hash[this.keys[i]];
            if (v)
                s[s.length] = v.toString();
            else
                s[s.length] = "null";

            if (i != this.keys.length-1)
                s[s.length] = ", ";
        }
    } catch(e) {
        //do nothing here :-)
    }finally{
        s[s.length] = "}";
    }

    return s.join("");
}

/**
 * add
 * Concatanates hashtable to another hashtable.
 */
Hashtable.prototype.add = function(ht){
    try {
        ht.moveFirst();
        while(ht.next()){
            var key = ht.getKey();
            //put the new value in both cases (exists or not).
            this.hash[key] = ht.getValue();
            //but if it is a new key also increase the key set
            if (this.get(key) != null){
                this.keys[this.keys.length] = key;
            }
        }
    } catch(e) {
        //do nothing here :-)
    } finally {
        return this;
    }
};