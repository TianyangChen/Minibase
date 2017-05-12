#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
	slot_t* slot=new slot_t[1];
	slot[0].offset=-1;
	slot[0].length=-1;
	for (int i = 0; i != MAX_SPACE - DPFIXED;++i)
	{
		data[i]=' ';
	}
	slotCnt=0;
	usedPtr=MAX_SPACE - DPFIXED;
	freeSpace=MAX_SPACE - DPFIXED+ sizeof(slot_t);
	prevPage=INVALID_PAGE;
	nextPage=INVALID_PAGE;
	curPage=pageNo;
  // fill in the body
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;
    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{

	return prevPage;
    // fill in the body

}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
	prevPage=pageNo;
	return;
    // fill in the body
}

// **********************************************************
PageId HFPage::getNextPage()
{
	return nextPage;
    // fill in the body

}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
	nextPage=pageNo;
	// fill in the body
	return;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
	//Scan whether there is empty slots
	//if so, need reclen freespaces;
	//write on a given slot;
	//if no empty slots. use a new slot;
	//need reclen+8 freespaces;
	//write on a new slot;
	short index=slotCnt;
	short spaceReq=recLen+sizeof(slot_t);
	bool addSlot_flag=True;
	if (spaceReq>this->available_space()+4)
	{
	for (int i = 0 ; i != slotCnt; ++i)
	{
		if (slot[i].length==-1)
		{
//			Write on an empty slot;
			spaceReq=recLen;
			index=i;
			addSlot_flag=False;
			break;
		}
	}
	}

	if (spaceReq>this->available_space())
	{
//		Not enough space
		return DONE;
	}

//	write on disk;
	slot[index].offset=usedPtr-recLen;
	slot[index].length=recLen;
	for (int i =0 ; i != recLen; ++i)
	{
		data[usedPtr-recLen+i]=recPtr[i];
	}
//	update member parameters;

	usedPtr=usedPtr-recLen;
	freeSpace=freeSpace-spaceReq;
	rid.pageNo=curPage;
	rid.slotNo=index;
	if (addSlot_flag)
		{
			++slotCnt;
		}
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    // fill in the body
//	Find the record
	if (curPage!= rid.pageNo)
	{
		return FAIL;
	}
	if (rid.slotNo >= slotCnt||rid.slotNo<0)
	{
//		No slotNo
		return FAIL;
	}
//	move data
	slot_t curSlot=slot[rid.slotNo];

//	If there is no record on this slot, do nothing.
	if (curSlot.length==-1)
	{
//		redundency
		if (rid.slotNo==slotCnt-1)
		{
			--slotCnt;
			freeSpace=freeSpace+sizeof(slot_t);
		}
		return OK;
	}

	memmove(data+usedPtr+curSlot.length,data+usedPtr, curSlot.offset-usedPtr);

	for (int i =0; i != curSlot.length; ++i)
	{
		data[usedPtr+i]=' ';
	}
//	update parameters;
	usedPtr=usedPtr+curSlot.length;
	freeSpace=freeSpace+curSlot.length;
	slot[rid.slotNo].length=-1;

	for (int i = 0 ;i!=slotCnt;++i)
	{
		if (slot[i].offset<=curSlot.offset)
		{
			slot[i].offset=slot[i].offset+curSlot.length;
		}
	}

	int tempslot=rid.slotNo;
	while (tempslot==slotCnt-1 && tempslot>-1 && slot[tempslot].length==-1)
	{
		slot[tempslot].offset=-1;
		freeSpace=freeSpace+sizeof(slot_t);
		if (tempslot==0){
		}
		--slotCnt;
		--tempslot;
	}
	return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    // fill in the body
	firstRid.pageNo=curPage;
	for (int i = 0 ; i != slotCnt; ++i)
	{
		if (slot[i].length!=-1)
		{
			firstRid.slotNo=i;
			return OK;
		}
	}
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    // fill in the body

	if ( curRid.slotNo<0 ||curRid.slotNo>slotCnt-1)
	{
		return FAIL;
	}
	if (curRid.slotNo==slotCnt-1)
	{
		return DONE;
	}
	for (int i = curRid.slotNo+1; i != slotCnt; ++i)
	{
		if (slot[i].length!=-1)
		{
			nextRid.slotNo=i;
			nextRid.pageNo=curPage;
			return OK;
		}
	}
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // fill in the body
	if (rid.pageNo!=curPage)
		{
			return FAIL;
		}
	if (rid.slotNo>=slotCnt)
		{
			return FAIL;
		}
	slot_t curSlot=slot[rid.slotNo];
	memcpy(recPtr,data+curSlot.offset,curSlot.length);
	recLen=curSlot.length;
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    // fill in the body
	if (rid.pageNo!=curPage)
		{
			return FAIL;
		}
	if (rid.slotNo>=slotCnt)
		{
			return FAIL;
		}
	slot_t curSlot=slot[rid.slotNo];
	recPtr=data+curSlot.offset;
	recLen=curSlot.length;
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
		short* slotPtr = new short;
		slotPtr = (short*)slot;

		// check whether there are empty slot
		for(int i = 0; i < slotCnt; i++)
		{
			if (*(slotPtr+i*2+1) == EMPTY_SLOT)
			{
				return freeSpace;
			}
		}

		if(freeSpace < sizeof(struct slot_t))
			return 0;
		else
			return (freeSpace - sizeof(struct slot_t));
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
	if (slotCnt==0)
	{return True;}
	return False;
}



