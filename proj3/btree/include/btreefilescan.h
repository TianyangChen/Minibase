/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */
 
#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"

// errors from this class should be defined in btfile.h

class BTreeFileScan : public IndexFileScan {
public:
    friend class BTreeFile;



    // get the next record
    Status get_next(RID & rid, void* keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    // destructor
    ~BTreeFileScan();

    bool keycompare(const void* keyptr);

    int keySize;

    AttrType keytype;

    Keytype* lokey;

    Keytype* hikey;

    RID currid;

    PageId curpageid;

    BTLeafPage* curpage;


private:


};

#endif
