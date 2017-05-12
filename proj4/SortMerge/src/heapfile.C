#include "heapfile.h"
#include <typeinfo>

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

//extern SystemDefs* minibase_globals;

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
    // fill in the body

	if (name==0)
	{
		returnStatus=DONE;
		return;
	}
	fileName=0;
	char * fileName = (char*) malloc(strlen(name) + 1);
	strcpy(fileName, name);
	file_deleted=false;
	Status status;
	status=MINIBASE_DB->get_file_entry(name,firstDirPageId);
	if (status!=OK)
	{
		Page* DirPagePtr=new Page;
		HFPage* DirPage=new HFPage;
	    status=MINIBASE_BM->newPage(firstDirPageId,DirPagePtr);
	    DirPage=reinterpret_cast<HFPage*>(DirPagePtr);
	    DirPage->init(firstDirPageId);
	    status=MINIBASE_DB->add_file_entry(fileName,firstDirPageId);
	    status=MINIBASE_BM->unpinPage(firstDirPageId);
	    returnStatus = OK;
	    return;
	}
	returnStatus=OK;
	return;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // fill in the body
	Status status;
	DataPageInfo * pageInfo=new DataPageInfo;
	PageId DirPageId;
	RID dataPageRid;
	status=allocateDirSpace(pageInfo,DirPageId,dataPageRid);
	for (int i = 0 ; i != dataPageRid.pageNo; ++i)
	{
	}
	PageId dirPageID=firstDirPageId;
	PageId nextDirPageID;
	Page* dirPagePtr=new Page;
	HFPage* dirPage=new HFPage;
	do
	{
		status=MINIBASE_BM->pinPage(dirPageID,dirPagePtr);
		dirPage=reinterpret_cast<HFPage*>(dirPagePtr);
		nextDirPageID=dirPage->getNextPage();
		if (nextDirPageID==-1)
		{
			fileName=0;
			file_deleted=true;
			firstDirPageId=INVALID_PAGE;
			status=MINIBASE_BM->unpinPage(dirPageID);
			return;
		}
		status=MINIBASE_BM->unpinPage(dirPageID);
		status=MINIBASE_BM->flushPage(dirPageID);
		dirPageID=nextDirPageID;
	} while(1);

}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
   // fill in the body
//	Add all records in directory pages.
	Status status;

	DataPageInfo* pageInfo=new DataPageInfo[1];
	int dirNo;
	RID pageInfoRid;
	int recCnt=0;
	status=allocateDirSpace(pageInfo,dirNo,pageInfoRid);
	for (int i =0; i !=pageInfoRid.pageNo;++i)
	{
		recCnt=recCnt+pageInfo[i].recct;
	}
   return recCnt;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    // fill in the body

	Status status;
	DataPageInfo* pageInfo=new DataPageInfo;
	int dirNo;
	RID pageInfoRid;
	bool avalPage_flag=false;
	Page* curPagePtr=new Page;
	HFPage* curPage=new HFPage;
	DataPageInfo* curPageInfo=new DataPageInfo;
	if (recLen>1000)
	{
		MINIBASE_FIRST_ERROR(HEAPFILE,3);
		return HEAPFILE;
	}
	status=allocateDirSpace(pageInfo, dirNo, pageInfoRid);
	for(int i = 0 ; i!=pageInfoRid.pageNo;++i)
	{
		if (pageInfo[i].availspace>=recLen+4)
		{

			status=MINIBASE_BM->pinPage(pageInfo[i].pageId,curPagePtr);
			curPage=reinterpret_cast<HFPage*>(curPagePtr);
			status=curPage->insertRecord(recPtr,recLen,outRid);
			if (status!=OK)
			{
				cout<<"Error: insertRecord: cannot insert give dpinfo from allocate"<<endl;
			}
			curPageInfo=pageInfo+i*sizeof(DataPageInfo);
			avalPage_flag=true;
			status=MINIBASE_BM->unpinPage(pageInfo[i].pageId);
			break;
		}
	}

	if (avalPage_flag==false)
	{

//		No page could contain the record
		status=newDataPage(curPageInfo);
		if (status==FAIL)
		{
			return FAIL;
		}
		if (status==DONE)
			return DONE;
		status=MINIBASE_BM->pinPage(curPageInfo->pageId,curPagePtr);
		curPage=reinterpret_cast<HFPage*>(curPagePtr);
		status=curPage->insertRecord(recPtr,recLen,outRid);
		if (status!=OK)
		{
			status=MINIBASE_BM->unpinPage(curPageInfo->pageId);
			cout<<"Error: insertRecord: failed to insert a record."<<endl;
			return FAIL;
		}
		status=MINIBASE_BM->unpinPage(curPageInfo->pageId);
	}

//	update directory page
	PageId curDirID;
	HFPage* curDirPage=new HFPage;
	PageId curDataPageId;
	RID dirRid;
	HFPage* curPage2=new HFPage;
	char* dirEntry=new char;
	int entryLen;
	status=findDataPage(outRid,curDirID,curDirPage,curDataPageId,curPage2,dirRid);
	status=curDirPage->returnRecord(dirRid,dirEntry,entryLen);

	DataPageInfo* dpInfo=new DataPageInfo;
	dpInfo=reinterpret_cast<DataPageInfo*>(dirEntry);
	++(dpInfo->recct);
	dpInfo->availspace=curPage2->available_space();

	if (status!=OK)
	{
		cout<<"Notice! This entry structure cannot be used."<<endl;
	}
	status=MINIBASE_BM->unpinPage(curDirID);
	status=MINIBASE_BM->unpinPage(curDataPageId);
    return OK;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  // fill in the body
	Status status;
	PageId rpDirPageId;
	HFPage* rpdirpage=new HFPage;
	PageId rpDataPageId;
	HFPage* rpdatapage=new HFPage;
	RID rpDataPageRid;
	status=findDataPage(rid,rpDirPageId,rpdirpage,rpDataPageId,rpdatapage,rpDataPageRid);
	if (status!=OK)
	{
		cout<<"cannot find the data page in directory page"<<endl;
		status=MINIBASE_BM->unpinPage(rpDataPageId);
		status=MINIBASE_BM->unpinPage(rpDirPageId);
		return DONE;
	}
	status=rpdatapage->deleteRecord(rid);

	if (status!=OK)
	{
		cout<<"cannot delete record in datapage"<<endl;
		status=MINIBASE_BM->unpinPage(rpDataPageId);
		status=MINIBASE_BM->unpinPage(rpDirPageId);
		return DONE;
	}
	char* recPtr;
	int recLen;
	status=rpdirpage->returnRecord(rpDataPageRid,recPtr,recLen);
	DataPageInfo* pageinfo;
	pageinfo=reinterpret_cast<DataPageInfo*>(recPtr);
	pageinfo->availspace=rpdatapage->available_space();
	pageinfo->recct--;
	if (pageinfo->recct==0)
	{
//		The page is empty
		status=MINIBASE_BM->unpinPage(rid.pageNo);
		status=MINIBASE_BM->freePage(rid.pageNo);
		rpdirpage->deleteRecord(rpDataPageRid);
	}
	if (rpdirpage->empty())
	{
//		the directory page is empty
		cout<<"Directory page is empty"<<endl;
		PageId prevPage=rpdirpage->getPrevPage();
		PageId nextPage=rpdirpage->getNextPage();
		Page* curPagePtr=new Page;
		HFPage* curPage=new HFPage;

		if (prevPage!=-1)
		{
			status=MINIBASE_BM->pinPage(prevPage,curPagePtr);
			curPage=reinterpret_cast<HFPage*>(curPagePtr);
			curPage->setNextPage(nextPage);
			status=MINIBASE_BM->unpinPage(prevPage);
		}
		if (nextPage!=-1)
		{
			status=MINIBASE_BM->pinPage(nextPage,curPagePtr);
			curPage=reinterpret_cast<HFPage*>(curPagePtr);
			curPage->setPrevPage(prevPage);
			status=MINIBASE_BM->unpinPage(nextPage);
		}
		status=MINIBASE_BM->unpinPage(rpDirPageId);
		status=MINIBASE_BM->freePage(rpDirPageId);
	}
	else
	{
		status=MINIBASE_BM->unpinPage(rpDirPageId);
		status=MINIBASE_BM->unpinPage(rpDataPageId);
	}
  return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
  // fill in the body
	PageId rpDirPageId;
	HFPage* rpdirpage=new HFPage;
	PageId rpDataPageId;
	HFPage* rpdatapage=new HFPage;
	RID rpDataPageRid;
	Status status;
	status=findDataPage(rid,rpDirPageId,rpdirpage,rpDataPageId, rpdatapage,rpDataPageRid);
	if (status!=OK)
	{
		cout<<"Cannot locate the record"<<endl;
		return DONE;
	}
	int orgRecLen;
	char* orgRecPtr=new char;
	status=rpdatapage->returnRecord(rid,orgRecPtr,orgRecLen);
	if (orgRecLen!=recLen)
		{
		status=MINIBASE_FIRST_ERROR(HEAPFILE,1);
		status=MINIBASE_BM->unpinPage(rpDirPageId);
			status=MINIBASE_BM->unpinPage(rpDataPageId);
			return HEAPFILE;
		}
	memcpy(orgRecPtr,recPtr,recLen);
	status=MINIBASE_BM->unpinPage(rpDirPageId);
	status=MINIBASE_BM->unpinPage(rpDataPageId);
	return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  // fill in the body 
	PageId rpDirPageId;
	HFPage* rpdirpage=new HFPage;
	PageId rpDataPageId;
	HFPage* rpdatapage=new HFPage;
	RID rpDataPageRid;
	Status status;
	status=findDataPage(rid,rpDirPageId,rpdirpage,rpDataPageId, rpdatapage,rpDataPageRid);
	if (status!=OK)
	{
		cout<<"Cannot locate the record"<<endl;
		return DONE;
	}
	char* orgRecPtr;
	status=rpdatapage->returnRecord(rid,orgRecPtr,recLen);
	orgRecPtr=new char[recLen];
	memcpy(recPtr,orgRecPtr,recLen);
	status=MINIBASE_BM->unpinPage(rpDirPageId);
	status=MINIBASE_BM->unpinPage(rpDataPageId);

  return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
	return new Scan(this,status);
}


// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    // fill in the body
	Status status;
	DataPageInfo * pageInfo=new DataPageInfo;
	PageId DirPageId;
	RID dataPageRid;
	status=allocateDirSpace(pageInfo,DirPageId,dataPageRid);
	for (int i = 0 ; i != dataPageRid.pageNo; ++i)
	{
		status=MINIBASE_BM->freePage(pageInfo[i].pageId);
	}
	PageId dirPageID=firstDirPageId;
	PageId nextDirPageID;
	Page* dirPagePtr=new Page;
	HFPage* dirPage=new HFPage;
	do
	{
		status=MINIBASE_BM->pinPage(dirPageID,dirPagePtr);
		dirPage=reinterpret_cast<HFPage*>(dirPagePtr);
		nextDirPageID=dirPage->getNextPage();
		if (nextDirPageID==-1)
		{
			return OK;
		}
		status=MINIBASE_BM->freePage(dirPageID);
		dirPageID=nextDirPageID;
	} while(1);
	fileName=0;
	file_deleted=true;
	firstDirPageId=INVALID_PAGE;
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(struct DataPageInfo *dpinfop)
{
//  Grab a page from buffer manager;
//	notice: the new datapage is pinned;
	Status status;
	PageId DirPageID = firstDirPageId;
	PageId nextDirPageID;
	Page* DirPagePtr=new Page;
	HFPage* DirPage=new HFPage;
	int newPageID;
	Page* newPagePtr=new Page;
	HFPage* newPage=new HFPage;
	int DirFreeSpace;
	int newDirPageID;
	Page* newDirPagePtr=new Page;
	HFPage* newDirPage=new HFPage;

	status=MINIBASE_BM->newPage(newPageID,newPagePtr);
	newPage=reinterpret_cast<HFPage*>(newPagePtr);
	newPage->init(newPageID);

	int freespace=newPage->available_space();
	int entryLen;
	char* dirtEntry=new char[0];
	RID dataPageRID;
	dpinfop->pageId=newPageID;
	dpinfop->availspace =newPage->available_space();
	dpinfop->recct = 0;
	dirtEntry=reinterpret_cast<char*>(dpinfop);
	entryLen=sizeof(DataPageInfo);
	do
	{
		status=MINIBASE_BM->pinPage(DirPageID,DirPagePtr);
		DirPage=reinterpret_cast<HFPage*>(DirPagePtr);
		DirFreeSpace=DirPage->available_space();
		if(DirFreeSpace<entryLen)
		{
//			Cannot write in that page.
			nextDirPageID=DirPage->getNextPage();
			if (nextDirPageID!=-1)
			{
//				This is not the last directory page, find the next one.
				status=MINIBASE_BM->unpinPage(DirPageID);
				DirPageID=nextDirPageID;
				continue;
			}
//			This is the last directory page, start a new page and write on
//			a new directory page.
			status=MINIBASE_BM->newPage(newDirPageID,newDirPagePtr);
			DirPage->setNextPage(newDirPageID);
			status=MINIBASE_BM->unpinPage(DirPageID);
//			return FAIL;
			newDirPage=reinterpret_cast<HFPage*>(newDirPagePtr);
			newDirPage->init(newDirPageID);
			newDirPage->setPrevPage(DirPageID);
			DirPageID=newDirPageID;
			DirPagePtr=newDirPagePtr;
			DirPage=newDirPage;
			status=DirPage->insertRecord(dirtEntry,entryLen,dataPageRID);
			if (status!=OK)
			{
				dpinfop->availspace=0;
				dpinfop->pageId=-1;
				dpinfop->recct=-1;
				status=MINIBASE_BM->unpinPage(newPageID);
				status=MINIBASE_BM->unpinPage(newDirPageID);
				return FAIL;
			}

			status=MINIBASE_BM->unpinPage(newPageID);
			status=MINIBASE_BM->unpinPage(newDirPageID);

			return OK;
		}

//		found the directory page that can write the entry in
		status=DirPage->insertRecord(dirtEntry,entryLen,dataPageRID);
		if (status!=OK)
		{
			cout<<"Error: newDataPage: can not insert dir entry record"<<endl;
			dpinfop->availspace=0;
			dpinfop->pageId=-1;
			dpinfop->recct=-1;
			status=MINIBASE_BM->unpinPage(newPageID);
			status=MINIBASE_BM->unpinPage(newDirPageID);
			return FAIL;
		}
		status=MINIBASE_BM->unpinPage(newPageID);
		status=MINIBASE_BM->unpinPage(DirPageID);
		return OK;
	} while(1);
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
	Status status;
	Page* DirPagePtr=new Page;
	PageId nextDirPageID;
	char* recPtr=new char[0];
	int recLen;
	HFPage* DirPage=new HFPage;
	PageId DirPageID=firstDirPageId;
    int curPageID;
    RID tmpRID;
    RID tmpRID2;
    Page* curPagePtr=new Page;
	do
	{
		status=MINIBASE_BM->pinPage(DirPageID,DirPagePtr);
		if (status!=OK)
		{
//			No more directory page found.
			rpDirPageId=INVALID_PAGE;
			rpdirpage=0;
			rpDataPageId=INVALID_PAGE;
			rpdatapage=0;
			rpDataPageRid.pageNo=-1;
			rpDataPageRid.slotNo=-1;
			status=MINIBASE_BM->unpinPage(DirPageID);
			return DONE;
		}
		DirPage=reinterpret_cast<HFPage*>(DirPagePtr);
    	status=DirPage->firstRecord(tmpRID);
    	if (status!=OK)
//    		Redundency, an empty directory page is found.
    	{
    		cout<<"Error: FindDataPage find an empty directory page."<<endl;
    		rpDirPageId=INVALID_PAGE;
    		rpdirpage=0;
    		rpDataPageId=INVALID_PAGE;
    		rpdatapage=0;
    		rpDataPageRid.pageNo=-1;
    		rpDataPageRid.slotNo=-1;
    		status=MINIBASE_BM->unpinPage(DirPageID);
    		return DONE;
    	}

    	do
//    	Check every record. If one record match, return.
//    	if no record match, turn to next directory page.
    	{
    		status=DirPage->returnRecord(tmpRID,recPtr,recLen);
    		DataPageInfo* dpinfo=new DataPageInfo;
    		dpinfo=reinterpret_cast<DataPageInfo*>(recPtr);
    		curPageID=dpinfo->pageId;
    		if (curPageID==rid.pageNo)
    		{
//    			Found it!! return all information
    			rpDirPageId=DirPageID;
    			rpdirpage=DirPage;
    			rpDataPageId=curPageID;
    			status=MINIBASE_BM->pinPage(curPageID,curPagePtr);
    			rpdatapage=reinterpret_cast<HFPage*>(curPagePtr);
    			rpDataPageId=curPageID;
    			rpDataPageRid=tmpRID;
    			return OK;
    		}

    		status=DirPage->nextRecord(tmpRID,tmpRID2);
    		if (status!=OK)
    		{
//    			already arrived at the end of page, turn to next page
    			nextDirPageID=DirPage->getNextPage();
    			if (nextDirPageID==-1)
    			{
    				rpDirPageId=INVALID_PAGE;
    				rpdirpage=0;
    				rpDataPageId=INVALID_PAGE;
    				rpdatapage=0;
    				rpDataPageRid.pageNo=-1;
    				rpDataPageRid.slotNo=-1;
    				status=MINIBASE_BM->unpinPage(DirPageID);
    				return DONE;
    			}
    			break;
    		}
    		if (status==OK)
    			tmpRID=tmpRID2;
    	} while (1);
//    	restore the pin count and get to the next page.
    	status=MINIBASE_BM->unpinPage(DirPageID);
		DirPageID=nextDirPageID;
	} while (1);

}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // fill in the body
	Status status;
	PageId dirPageID=firstDirPageId;
	Page* dirPagePtr=new Page;
	HFPage* dirPage=new HFPage;
	RID dirRid;
	RID nextDirRid;
	char* dirEntry=0;
	int entryLen;
	PageId nextDirPageID;
	int curPageID;
	int recCnt;
	int avalSpace;
	int pageIndex=0;
	int dirNo=0;
	do
	{
		status=MINIBASE_BM->pinPage(dirPageID,dirPagePtr);
		dirPage=reinterpret_cast<HFPage*>(dirPagePtr);
		dirRid.pageNo=dirPageID;
		status=dirPage->firstRecord(dirRid);
		if (status!=OK && dirPageID==firstDirPageId)
		{
//			Totally new file
			status=MINIBASE_BM->unpinPage(dirPageID);
			dpinfop=0;
			allocDirPageId=dirPageID;
			allocDataPageRid=dirRid;
			allocDataPageRid.pageNo=pageIndex;
			return OK;
		}
		if (status!=OK)
		{
			cout<<"Error: FindDataPage: an empty directory page exist"<<endl;
			return FAIL;
		}
		do{
			status=dirPage->returnRecord(dirRid,dirEntry,entryLen);
			DataPageInfo* dpinfo=new DataPageInfo;
			dpinfo=reinterpret_cast<DataPageInfo*>(dirEntry);
			curPageID=dpinfo->pageId;
			recCnt=dpinfo->recct;
			avalSpace=dpinfo->availspace;
			dpinfop[pageIndex].availspace=avalSpace;
			dpinfop[pageIndex].pageId=curPageID;
			dpinfop[pageIndex].recct=recCnt;
			++pageIndex;
			status=dirPage->nextRecord(dirRid,nextDirRid);

			if (status!=OK)
			{
				break;
			}
			dirRid=nextDirRid;
		} while(1);
		nextDirPageID=dirPage->getNextPage();
		if (nextDirPageID==-1)
		{
			allocDirPageId=dirNo;
			allocDataPageRid=dirRid;
			allocDataPageRid.pageNo=pageIndex;
			status=MINIBASE_BM->unpinPage(dirPageID);
			return OK;
		}

		++dirNo;
		status=MINIBASE_BM->unpinPage(dirPageID);
		dirPageID=nextDirPageID;

	} while(1);
}

// *******************************************
