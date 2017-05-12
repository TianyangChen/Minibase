/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"
// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
  // put your code here
	dataPageId=-1;
	dataPageRid.pageNo=-1;
	dataPageRid.slotNo=-1;
	dirPage=0;
	dataPage=0;
	userRid.pageNo=-1;
	userRid.slotNo=-1;
	scanIsDone=0;
	nxtUserStatus=-1;
	_hf=hf;
	dirPageId=hf->firstDirPageId;
	status=firstDataPage();
	RID nextRid;

	status=dataPage->nextRecord(userRid,nextRid);
	if (status!=OK && userRid.slotNo!=-1)
	{
		cout<<"Scan::init: there are no more records in the dataPage"<<endl;
		nxtUserStatus=0;
	}
	else
	{
		nxtUserStatus=1;
	}
  status = OK;
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
  // put your code here
	Status status;
	status=reset();
	_hf=0;
}


// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
  // put your code here
	Status status;
	RID nextRid;

	if (userRid.slotNo==-1)
	{
		status=dataPage->firstRecord(userRid);
	}
	else
	{
		if (nxtUserStatus==0){
			status=nextDataPage();
			if (status==DONE)
			{
				status=MINIBASE_BM->unpinPage(dirPageId);
				status=MINIBASE_BM->unpinPage(dataPageId);
				nxtUserStatus=0;
				return DONE;
			}
			if (status==FAIL)
			{
				return FAIL;
			}
		}
		else
		{
			status=dataPage->nextRecord(userRid,nextRid);
			userRid=nextRid;
		}
	}
	char* curRecPtr;
	status=dataPage->returnRecord(userRid,curRecPtr,recLen);
	struct Rec
	{
		int ival;
		float fval;
		char name[24];
	};
	Rec* rec=new Rec;
	rec=reinterpret_cast<Rec*>(curRecPtr);
	status=dataPage->nextRecord(userRid,nextRid);
	if (status!=OK)
	{
		nxtUserStatus=0;
	}
	else
	{
		nxtUserStatus=1;
	}
	rid=userRid;
	memcpy(recPtr,curRecPtr,recLen);
	return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
  // put your code here
	Status status;
	status=reset();
	_hf=hf;
	RID nextRid;
	status=dataPage->nextRecord(userRid,nextRid);
	if (status!=OK)
	{
		cout<<"Scan::init: there are no more records in the dataPage"<<endl;
		nxtUserStatus=0;
	}
	else
	{
		nxtUserStatus=1;
	}
	return OK;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
  // put your code here
	Status status;
	if (nxtUserStatus==1)
	{
		status=MINIBASE_BM->unpinPage(dirPageId);
		status=MINIBASE_BM->unpinPage(dataPageId);
	}

	dirPage=0;
	dataPage=0;
	userRid.pageNo=-1;
	userRid.slotNo=-1;
	dirPageId=-1;
	dataPageId=-1;
	dataPageRid.pageNo=-1;
	dataPageRid.slotNo=-1;
	scanIsDone=0;
	nxtUserStatus=0;
	return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
  // put your code here
	Status status;
	dirPage=0;
	dataPage=0;
	userRid.pageNo=-1;
	userRid.slotNo=-1;
	dirPageId=-1;
	dataPageId=-1;
	dataPageRid.pageNo=-1;
	dataPageRid.slotNo=-1;
	scanIsDone=0;
	nxtUserStatus=1;

	dirPageId=_hf->firstDirPageId;
	Page* curPagePtr;
	status=MINIBASE_BM->pinPage(dirPageId,curPagePtr);
	dirPage=reinterpret_cast<HFPage*>(curPagePtr);
	status=dirPage->firstRecord(dataPageRid);
	if (status!=OK)
		{
			cout<<"Scan: the dirPage is empty"<<endl;
			return DONE;
		}
	DataPageInfo* curinfo;
	char* curInfoPtr;
	int infoLen;
	status=dirPage->returnRecord(dataPageRid,curInfoPtr,infoLen);
	curinfo=reinterpret_cast<DataPageInfo*>(curInfoPtr);
	dataPageId=curinfo->pageId;
	status=MINIBASE_BM->pinPage(dataPageId,curPagePtr);
	dataPage=reinterpret_cast<HFPage*>(curPagePtr);
	status=dataPage->firstRecord(userRid);
	if (status!=OK)
	{
		cout<<"Scan: the datapage is empty!"<<endl;
		return DONE;
	}
	userRid.slotNo=-1;
	return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
  // put your code here
	Status status;

	RID nextDataRid;
	status=dirPage->nextRecord(dataPageRid,nextDataRid);
	if (status!=OK)
	{
		status=nextDirPage();
		if (status==FAIL)
		{
			return FAIL;
		}
		if (status==DONE)
		{
			return DONE;
		}

		return OK;
	}
	status=MINIBASE_BM->unpinPage(dataPageId);
	dataPageRid=nextDataRid;
	char* curInfoPtr;
	int curLen;
	status=dirPage->returnRecord(dataPageRid,curInfoPtr,curLen);
	if (status!=OK)
	{
		cout<<"Scan:  nextdatapage: Cannot return record"<<endl;
	}
	DataPageInfo* curinfo;
	curinfo=reinterpret_cast<DataPageInfo*>(curInfoPtr);
	dataPageId=curinfo->pageId;
	Page* curPagePtr;
	status=MINIBASE_BM->pinPage(dataPageId,curPagePtr);
	dataPage=reinterpret_cast<HFPage*>(curPagePtr);
	status=dataPage->firstRecord(userRid);
	if (status!=OK)
	{
		cout<<"Scan: the datapage is empty!"<<endl;
		return FAIL;
	}
	return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
  // put your code here
	Status status;
	PageId nextDirPageId;
	Page* curPagePtr;
	nextDirPageId=dirPage->getNextPage();
	if (nextDirPageId==-1)
	{
//		means there are no more pages
		return DONE;
	}
	status=MINIBASE_BM->unpinPage(dirPageId);
	status=MINIBASE_BM->unpinPage(dataPageId);
	dirPage=0;
	dirPageId=nextDirPageId;
	dirPage=0;
	dataPageId=-1;
	status=MINIBASE_BM->pinPage(dirPageId,curPagePtr);
	dirPage=reinterpret_cast<HFPage*>(curPagePtr);
	status=dirPage->firstRecord(dataPageRid);
	if (status!=OK)
		{
			cout<<"Scan: the dirPage is empty!"<<endl;
			return FAIL;
		}
	DataPageInfo* curinfo;
	char* curInfoPtr;
	int infoLen;
	status=dirPage->returnRecord(dataPageRid,curInfoPtr,infoLen);
	curinfo=reinterpret_cast<DataPageInfo*>(curInfoPtr);
	dataPageId=curinfo->pageId;
	status=MINIBASE_BM->pinPage(dataPageId,curPagePtr);
	dataPage=reinterpret_cast<HFPage*>(curPagePtr);
	status=dataPage->firstRecord(userRid);
		if (status!=OK)
		{
			cout<<"Scan: the datapage is empty!"<<endl;
			return FAIL;
		}

  return OK;
}
