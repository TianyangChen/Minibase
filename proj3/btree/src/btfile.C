/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
  // put your code here
	destroyed=false;
	fileName=0;
	fileName = (char*) malloc(strlen(filename) + 1);
	strcpy(fileName, filename);
	Status status;
	PageId headerpageid;
	status=MINIBASE_DB->get_file_entry(filename,headerpageid);
	if (status!=OK)
	{
		returnStatus=status;
		rootPage=new BTIndexPage;
		headerpage=new HeaderPage;

		return;
	}
	this-> headerpage=new HeaderPage;
	status=MINIBASE_BM->pinPage(headerpageid,(Page*&)headerpage);
	type=headerpage->type;
	this->headerPageId=headerpageid;
	if (type!=0 && type!=1)
	{

		returnStatus=DONE;
		return;
	}
	if (type==0)
	{
		keySize=MAX_KEY_SIZE1;
	}
	else if (type ==1)
	{
		keySize=sizeof(int);
	}
	rootPage=new BTIndexPage;
	MINIBASE_BM->pinPage(headerpage->rootpageid,(Page*&)rootPage);
	rootPageId=headerpage->rootpageid;
	returnStatus=OK;
	return;
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
  // put your code here
	destroyed=false;
	if (filename==0)
	{
		returnStatus=DONE;
		return;
	}
	if (keytype!=attrString && keytype!=attrInteger)
	{
		returnStatus=DONE;
		return;
	}
	if (keytype==attrString && keysize!=MAX_KEY_SIZE1)
	{
		returnStatus=DONE;
		return;
	}
	if (keytype==attrInteger && keysize!=sizeof(int))
	{
		returnStatus=DONE;
		return;
	}
	fileName=0;
	fileName = (char*) malloc(strlen(filename) + 1);
	strcpy(fileName, filename);
	Status status;
	PageId headerpageid;
	status=MINIBASE_DB->get_file_entry(filename,headerpageid);
	this->headerPageId=headerpageid;

	if (status!=OK)
	{
//		New file
		this->headerpage=new HeaderPage;
		int headerpageid;
		MINIBASE_BM->newPage(headerpageid,(Page*&)headerpage);
		headerpage->type=keytype;
		type=keytype;
		this->rootPage=new BTIndexPage;
		MINIBASE_BM->newPage(rootPageId,(Page*&)rootPage);
		rootPage->init(rootPageId);
		headerpage->rootpageid=rootPageId;
		if (keytype==attrString)
		{
			keySize=MAX_KEY_SIZE1;
		}
		else if (keytype==attrInteger)
		{
			keySize=sizeof(int);
		}
		MINIBASE_DB->add_file_entry(filename,headerpageid);
		headerPageId=headerpageid;
		returnStatus=OK;
		return;
	}
//	existing file
	this->headerpage=new HeaderPage;
	status=MINIBASE_BM->pinPage(headerpageid,(Page*&)headerpage);
	this->type=headerpage->type;
	if (type!=0 && type!=1)
	{
		returnStatus=DONE;
		return;
	}
	if (type==0)
	{
		keySize=MAX_KEY_SIZE1;
	}
	else if (type==1)
	{
		keySize=sizeof(int);
	}
	if (type!=keytype)
	{
		returnStatus=DONE;
		return;
	}
	rootPage=new BTIndexPage;
	MINIBASE_BM->pinPage(headerpage->rootpageid,(Page*&)rootPage);
	rootPageId=headerpage->rootpageid;
	headerPageId=headerpageid;
	returnStatus=OK;
	return;
}

BTreeFile::~BTreeFile ()
{
  // put your code here
//	delete fileName[128];
	keySize=0;
	if (destroyed==false)
	{
		MINIBASE_BM->unpinPage(rootPageId);
		MINIBASE_BM->unpinPage(headerPageId);
	}
	rootPage=0;
}

Status BTreeFile::destroyFile ()
{
  // put your code here
	destroyed=true;
	vector<PageId> curlevelpage;
	curlevelpage.push_back(rootPageId);
	vector<PageId>::iterator i,j;
	vector<PageId> nextlevelpage;
	BTIndexPage* curindex;
	PageId curindexid;
	Keytype* key=new Keytype;
	PageId curpageid;
	Status status;
	int index=0;
	do
	{
		++index;
		for (i=curlevelpage.begin();i!=curlevelpage.end();++i)
		{

			curindexid=*i;
			MINIBASE_BM->pinPage(curindexid,(Page*&)curindex);
			if (curindex->get_type()==LEAF)
			{
				MINIBASE_BM->unpinPage(curindexid);
				for (j=curlevelpage.begin();j!=curlevelpage.end();++j)
				{
					MINIBASE_BM->freePage(*j);
				}
//				delete this;
				MINIBASE_DB->delete_file_entry(fileName);
				MINIBASE_BM->unpinPage(rootPageId);
				MINIBASE_BM->unpinPage(headerPageId);
				MINIBASE_BM->freePage(rootPageId);
				MINIBASE_BM->freePage(headerPageId);
				return OK;
			}
			RID rid;
			status=curindex->get_first(rid,key,curpageid);

			if (status==DONE)
			{
				MINIBASE_BM->unpinPage(curindexid);
				continue;
			}
			else if (status!=OK)
			{
				cout<<"reveal error"<<endl;
				MINIBASE_BM->unpinPage(curindexid);
				return OK;
			}

			nextlevelpage.push_back(curpageid);
			do
			{
				status=curindex->get_next(rid,key,curpageid);
				if (status==NOMORERECS)
				{
					MINIBASE_BM->unpinPage(curindexid);
					break;
				}
				nextlevelpage.push_back(curpageid);
			} while (1);
		}
		for (j=curlevelpage.begin();j!=curlevelpage.end();++j)
		{
			MINIBASE_BM->unpinPage(*j);
		}
		curlevelpage.clear();
		curlevelpage=nextlevelpage;
		nextlevelpage.clear();
	}while(1);
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  // put your code here
	Keytype* keym=new Keytype;
	if (type==attrString)
	{
		memcpy((char*)keym,(char*)key,strlen((char*)key)-1);
	}
	else
	{
		keym->intkey=*((int*)key);
	}
	Status status;
	if (rootPage->empty()==true)
	{
		BTLeafPage* leafpage=new BTLeafPage;
		PageId leafpageid;
		MINIBASE_BM->newPage(leafpageid,(Page*&)leafpage);
		leafpage->init(leafpageid);
		RID currid;
		status=leafpage->insertRec(keym,type,rid,currid);
		if (status!=OK)
		{
			return status;
		}

		status=rootPage->insertKey(keym,type,leafpageid,currid);

		RID rid2;
		Keytype* curkey=new Keytype;
		PageId pageno;
		rootPage->get_first(rid2,curkey,pageno);
		if (status!=OK)
		{
			return status;
		}
		MINIBASE_BM->unpinPage(leafpageid);
		return OK;
	}

	vector<PageId> indexlist;
	BTLeafPage* leafpage=new BTLeafPage;
	PageId leafid;
	RID currid;

	status=findLeaf(keym,leafid,indexlist,leafpage);
	PageId leafId=leafid;
	if (status!=OK)
	{
		return status;
	}
	RID rid2,datarid2;
	Keytype* key2=new Keytype;
	Keytype* keybefore=new Keytype;

	status=leafpage->get_first(rid2,keybefore,datarid2);
	status=leafpage->get_first(rid2,key2,datarid2);
	if (keyCompare(key2,keym,type)==0)
	{
		MINIBASE_BM->unpinPage(leafid);
		return OK;
	}
	do
	{
		status=leafpage->get_next(rid2,key2,datarid2);
		if (keyCompare(key2,keym,type)==0)
		{
			MINIBASE_BM->unpinPage(leafid);
			return OK;
		}
		if (status==NOMORERECS)
		{
			break;
		}
	}while(1);

	status=leafpage->insertRec(keym, type,rid,currid);

	if (status==FAIL)
	{
		MINIBASE_BM->unpinPage(leafid);
		return FAIL;
	}

	BTLeafPage* newleaf=new BTLeafPage;
	PageId newleafid;
	Keytype* newkey=new Keytype;
	Keytype* curkey=new Keytype;

	if (status==DONE)
	{
		if (type==attrString)
		{
			leafpage->compress();
		}
	status=splitLeafPage(leafid, leafpage,(void*&)curkey,newleafid,newleaf,(void*&)newkey,keym,rid,type);
	if (status!=OK)
	{
		MINIBASE_BM->unpinPage(leafid);
		MINIBASE_BM->unpinPage(newleafid);
		return FAIL;
	}
	MINIBASE_BM->unpinPage(leafid);
	MINIBASE_BM->unpinPage(newleafid);
	int index=1;
	PageId insertpageno=newleafid;
	Keytype* insertkey=newkey;
	Keytype* insertkey2=curkey;
	PageId insertpageno2=leafid;
	do
	{
		BTIndexPage* curindex=new BTIndexPage;
		PageId curindexid=indexlist[index];
		if (curindexid==rootPageId)
		{
			int numofrecs;
			status=rootPage->insertKey(insertkey,type,insertpageno,currid);
			if (status==OK)
			{
				if (rootPage->numberOfRecords()!=numofrecs)
				{
					break;
				}
				if (rootPage->numberOfRecords()==numofrecs)
				{
					status=rootPage->insertKey(insertkey2,type,insertpageno2,currid);
					if (status==OK)
					{
						break;
					}
				}
			}
			PageId newindexid;
			BTIndexPage* newindex=new BTIndexPage;
			Keytype* newkey=new Keytype;
			if (type==attrString)
			{
				rootPage->compress();
			}
			status=splitIndexPage(rootPageId,rootPage,(void*&)curkey,newindexid,newindex,(void*&)newkey,insertkey,insertpageno,type);

			PageId rootpageid=rootPageId;
			Keytype* curkey2=new Keytype;
			memcpy((char*)curkey2,(char*)curkey,sizeof(Keytype));
			MINIBASE_BM->unpinPage(rootPageId);
			MINIBASE_BM->unpinPage(newindexid);
			MINIBASE_BM->newPage(rootPageId,(Page*&)rootPage);
			rootPage->init(rootPageId);
			rootPage->insertKey(curkey2,type,rootpageid,currid);
			rootPage->insertKey(newkey,type,newindexid,currid);
			headerpage->rootpageid=rootPageId;
			break;
		}
		MINIBASE_BM->pinPage(curindexid,(Page*&)curindex);
		status=curindex->insertKey(insertkey,type,insertpageno,currid);
		if (status==OK)
		{
			MINIBASE_BM->unpinPage(curindexid);
			break;
		}
		BTIndexPage* newindex=new BTIndexPage;
		PageId newindexid;
		if (type==attrString)
		{
			curindex->compress();
		}
		status=splitIndexPage(curindexid,curindex,(void*&)curkey,newindexid,newindex,(void*&)newkey,insertkey,insertpageno,type);
		if (status!=OK)
		{
			MINIBASE_BM->unpinPage(curindexid);
			MINIBASE_BM->unpinPage(newindexid);
			return FAIL;
		}
		index++;
		insertpageno=newindexid;
		insertkey=newkey;
		MINIBASE_BM->unpinPage(curindexid);
		MINIBASE_BM->unpinPage(newindexid);
	}while (1);
	}
	RID datarid;
	curkey=new Keytype;
	MINIBASE_BM->pinPage(leafId,(Page*&)leafpage);
	leafpage->get_first(currid,curkey,datarid);
	KeyDataEntry* entry=new KeyDataEntry;
	Datatype data;
	data.pageNo=leafId;
	int* entry_len=new int;
	make_entry(entry,type,curkey,INDEX,data,entry_len);

	PageId curpageno=leafId;
	int index=1;
	MINIBASE_BM->unpinPage(leafId);
	do
	{
		Keytype* curkey2=new Keytype;
		BTIndexPage* indexpage=new BTIndexPage;
		PageId indexid=indexlist[index];
		PageId datapageno;
		MINIBASE_BM->pinPage(indexid,(Page*&)indexpage);
		status=indexpage->get_first(currid,curkey2,datapageno);
		if (status!=OK)
		{
			return status;
		}
		do
		{

			if (datapageno==curpageno)
			{
				break;
			}
			curkey2=new Keytype;
			status=indexpage->get_next(currid,curkey2,datapageno);

			if (status==NOMORERECS)
			{
				cout<<"Error: cannot find the record in index for page "<<endl;
				return FAIL;
			}
			if (status!=OK)
			{
				cout<<"Error: update key when inserting failed."<<endl;
				return FAIL;
			}

		}while(1);

		char* recPtr;
		int recLen;
		status=indexpage->returnRecord(currid,recPtr,recLen);

		if (recLen!=*entry_len && type==attrInteger)
		{
			cout<<"Error: Key size not match when updating key during insertion."<<endl;
			return FAIL;
		}
		if (type==attrInteger)
		{
			memcpy(recPtr,entry,*entry_len);
		}
		if (type==attrString)
		{
			status=indexpage->deleteRecord(currid);
			status=indexpage->insertKey(curkey,type,data.pageNo,currid);
			if (status!=OK)
			{
				return FAIL;
			}
		}

		status=indexpage->get_first(currid,curkey,datapageno);
		if (status!=OK)
		{
			return status;
		}
		entry=new KeyDataEntry;
		data.pageNo=indexid;
			make_entry(entry,type,curkey,INDEX,data,entry_len);
		curpageno=indexlist[index];
		index++;

		MINIBASE_BM->unpinPage(indexid);
		if (index==indexlist.size())
		{
			break;
		}

	} while(1);
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
	if (rootPage->empty()==true)
	{
		return DONE;
	}
	Status status;
	PageId leafid;
	vector<PageId> indexlist;
	Keytype* keytype=new Keytype;
	memcpy((char*)keytype,(char*)key,sizeof(Keytype));
	BTLeafPage* leafpage=new BTLeafPage;

	status=findLeaf(keytype,leafid,indexlist,leafpage);
	RID recrid;
	Keytype* curkey=new Keytype;
	RID datarid;
	status=leafpage->get_first(recrid,curkey,datarid);
	if (status!=OK)
	{
		return status;
	}
	if (type==attrString)
	{
		char* keym=new char[MAX_KEY_SIZE1];
		memcpy(keym,key,strlen((char*)key)-1);
		if (keyCompare(curkey,keym,(AttrType)type)==0)
		{
			status=leafpage->deleteRecord(recrid);
			if (status!=OK)
			{
				return status;

			}
			MINIBASE_BM->unpinPage(leafid);
			return OK;
		}
	}
	if (keyCompare(curkey,key,(AttrType)type)==0)
	{
		status=leafpage->deleteRecord(recrid);
		if (status!=OK)
		{
			return status;
		}
		MINIBASE_BM->unpinPage(leafid);
		return OK;
	}
	do
	{

		if (keyCompare(curkey,key,(AttrType)type)==0)
		{
			status=leafpage->deleteRecord(recrid);
			if (status!=OK)
			{
				return status;
			}
			MINIBASE_BM->unpinPage(leafid);
			return OK;
		}
		status=leafpage->get_next(recrid,curkey,datarid);
		if (status!=OK)
		{
			return DONE;
		}
	}while(1);
}
    

IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
	BTreeFileScan* btscan=new BTreeFileScan;
	btscan->keytype=this->type;
	if (type==attrInteger)
	{
		btscan->keySize=4;
	}
	else if (type==attrString)
	{
		btscan->keySize=MAX_KEY_SIZE1;
	}
	btscan->lokey=NULL;
	btscan->hikey=NULL;
	if (lo_key!=NULL)
	{
		btscan->lokey=new Keytype;
		if (type==attrString)
		{
			memcpy(btscan->lokey,lo_key,strlen((char*)lo_key)-1);
		}
		if (type==attrInteger)
		{
			memcpy(btscan->lokey,lo_key,sizeof(int));
		}
	}
	if (hi_key!=NULL)
	{
		btscan->hikey=new Keytype;
		if (type==attrString)
		{
			memcpy(btscan->hikey,hi_key,strlen((char*)hi_key)-1);
		}
		if (type==attrInteger)
		{
			memcpy(btscan->hikey,hi_key,sizeof(int));
		}
	}
	btscan->currid.slotNo=-1;
	Status status;
	PageId leafid;
	vector<PageId> indexlist;
	BTLeafPage* leafpage=new BTLeafPage;
	Keytype* searchkey=new Keytype;
	if (lo_key==NULL)
	{
		RID rid;
		PageId pageno;
		rootPage->get_first(rid,searchkey,pageno);
	}
	else
	{
		memcpy(searchkey,lo_key,sizeof(Keytype));
	}
	status=findLeaf(searchkey, leafid, indexlist,leafpage);
	btscan->curpageid=leafid;
	btscan->curpage=leafpage;
	btscan->currid.pageNo=leafid;
	return btscan;
}

int BTreeFile::keysize(){
  // put your code here
	return keySize;
}

Status BTreeFile::splitIndexPage(PageId splitpageid, BTIndexPage* splitpage, void* &splitkey, PageId& newpageid,
		BTIndexPage*& newpage, void*& newkey, const void* insertkey, int newdatapageno,AttrType keytype)
{

	MINIBASE_BM->newPage(newpageid,(Page*&)newpage);
	newpage->init(newpageid);
	int numrec=splitpage->numberOfRecords();
	Status status;
	RID rid;
	Keytype* key=new Keytype;
	PageId pageno;
	for (int i=0;i!=numrec/2;++i)
	{

		status=splitpage->get_first(rid,key,pageno);
		if (status!=OK)
		{
			return FAIL;
		}
		RID rid2;
		status=newpage->insertKey(key,keytype,pageno,rid2);

		RID rid3;
		PageId curpageno;
		Keytype* newkey=new Keytype;

		if (status!=OK)
		{
			return FAIL;

		}
		splitpage->deleteRecord(rid);
	}
	status=splitpage->get_first(rid,key,pageno);

	if (status!=OK)
		return FAIL;
	RID rid2;
	if (keyCompare(key,insertkey,keytype)>0)
	{
		status=newpage->insertKey(insertkey,keytype,newdatapageno,rid2);
		if (status!=OK)
		{
			return FAIL;
		}
	}
	else
	{
		status=splitpage->insertKey(insertkey,keytype,newdatapageno,rid2);
		if (status!=OK)
		{
			return FAIL;
		}
	}
	status=splitpage->get_first(rid,splitkey,pageno);

	if (status!=OK)
	{
		return FAIL;
	}
	status=newpage->get_first(rid,newkey,pageno);

	if (status!=OK)
	{
		return FAIL;
	}
	newpage->setPrevPage(splitpage->getPrevPage());
	newpage->setNextPage(splitpageid);
	splitpage->setPrevPage(newpageid);
	splitpage->compress();
	MINIBASE_BM->unpinPage(splitpageid);
	MINIBASE_BM->unpinPage(newpageid);

	return OK;
}

Status BTreeFile::splitLeafPage(PageId splitpageid, BTLeafPage* splitpage, void* &splitkey, PageId& newpageid,
        		BTLeafPage*& newpage, void*& newkey, const void* insertkey, RID data,AttrType keytype)
{
	MINIBASE_BM->newPage(newpageid,(Page*&)newpage);
	newpage->init(newpageid);
	int numrec=splitpage->numberOfRecords();

	Status status;
	RID rid;
	Keytype* key=new Keytype;
	RID datarid;
	for (int i = 0 ; i != numrec/2; ++i)
	{
		key=new Keytype;
		status=splitpage->get_first(rid,key,datarid);
		if (status!=OK)
			return FAIL;
		RID rid2;
		status=newpage->insertRec(key,keytype,datarid,rid2);
		if (status!=OK)
			return FAIL;
		status=splitpage->deleteRecord(rid);
	}
	status=splitpage->get_first(rid,key,datarid);
	if (status!=OK)
		return FAIL;
	RID rid2;

	if (keyCompare(key,insertkey,keytype)>0)
	{

		status=newpage->insertRec(insertkey,keytype,data,rid2);
		if (status!=OK)
			return FAIL;
	}
	else
	{
		status=splitpage->insertRec(insertkey,keytype,data,rid2);
		if (status!=OK)
			return FAIL;
	}
	if (status!=OK)
		return FAIL;
	status=newpage->get_first(rid,newkey,datarid);
	if (status!=OK)
		return FAIL;
	status=splitpage->get_first(rid,splitkey,datarid);
	if (status!=OK)
	{
		return FAIL;
	}
	newpage->setPrevPage(splitpage->getPrevPage());
	newpage->setNextPage(splitpageid);
	splitpage->setPrevPage(newpageid);
	BTLeafPage* curpage=new BTLeafPage;
	splitpage->compress();
	MINIBASE_BM->pinPage(newpage->getPrevPage(),(Page*&)curpage);
	curpage->setNextPage(newpageid);
	MINIBASE_BM->unpinPage(newpage->getPrevPage());
	MINIBASE_BM->unpinPage(splitpageid);
	MINIBASE_BM->unpinPage(newpageid);
	return OK;
}

	Status BTreeFile::findLeaf(const void* key,PageId& leafid, vector<PageId>& indexlist,
			BTLeafPage*& leafpage)
	{
		indexlist.clear();
		indexlist.insert(indexlist.begin(),rootPageId);
		Status status;
		RID currid;
		PageId curpageid;
		Keytype* curkey=new Keytype;
		BTIndexPage* curpage=rootPage;
		do
		{

			vector<PageId> pageidlist;
			status=curpage->get_first(currid,curkey,curpageid);
			if (status==DONE)
				return DONE;
			int index=0;
			pageidlist.push_back(curpageid);
			do
			{
				if (keyCompare(curkey,key,(AttrType)type)>0)
				{
					break;
				}
				status=curpage->get_next(currid,curkey,curpageid);
				if (status==NOMORERECS)
				{
					index++;
					break;
				}
				pageidlist.push_back(curpageid);
				++index;
			}while(1);
			index=index-1;
			if (index==-1)
			{
				index=0;
			}
			MINIBASE_BM->unpinPage(curpageid);
			curpageid=pageidlist[index];
			status=MINIBASE_BM->pinPage(curpageid,(Page*&)curpage);
			if (curpage->get_type()==LEAF)
			{
				leafid=curpageid;
				indexlist.insert(indexlist.begin(),curpageid);
				leafpage=(BTLeafPage*)curpage;
				vector<PageId>::iterator i ;
				return OK;
			}
			indexlist.insert(indexlist.begin(),curpageid);
		}while(1);
	}


void BTreeFile::reveal()
{
	vector<PageId> curlevelpage;
	curlevelpage.push_back(rootPageId);
	vector<PageId>::iterator i,j;
	vector<PageId> nextlevelpage;
	BTIndexPage* curindex;
	PageId curindexid;
	Keytype* key=new Keytype;
	PageId curpageid;
	Status status;
	int index=0;
	RID rid;
	char* currec;
	do
	{
		cout<<index<<"th level"<<endl;
		++index;
		for (i=curlevelpage.begin();i!=curlevelpage.end();++i)
		{

			curindexid=*i;
			MINIBASE_BM->pinPage(curindexid,(Page*&)curindex);
			if (curindex->get_type()==LEAF)
			{
				RID datarid;
				MINIBASE_BM->unpinPage(curindexid);
				BTLeafPage* leaf=new BTLeafPage;
				for (j=curlevelpage.begin();j!=curlevelpage.end();++j)
				{
					MINIBASE_BM->pinPage(*j,(Page*&)leaf);
					key=new Keytype;
					status=leaf->get_first(rid,key,datarid);
					if (status==DONE)
					{
						cout<<"|| ";
						MINIBASE_BM->unpinPage(*j);
						return;
					}
					else if (status!=OK)
					{
						cout<<"reveal error"<<endl;
						MINIBASE_BM->unpinPage(*j);
						return;
					}
					if (type==attrString)
					{

						cout<<leaf->page_no()<<": ";
						cout<<"|"<<key->charkey;
					}
					if (type==attrInteger)
					{
						cout<<leaf->getPrevPage()<<" "<<leaf->page_no()
								<<" "<<leaf->getNextPage()<<endl;
						cout<<"|"<<key->intkey;
					}

					do
					{
						key=new Keytype;
						status=leaf->get_next(rid,key,datarid);
						if (status==NOMORERECS)
						{
							cout<<"| "<<endl;
							MINIBASE_BM->unpinPage(*j);
							break;
						}
						if (type==attrString)
						{
							cout<<"|"<<(key->charkey);

						}
						if (type==attrInteger)
						{
							cout<<"|"<<key->intkey;
						}
					} while (1);
					cout<<endl;
				}
				return;
			}
			key=new Keytype;
			status=curindex->get_first(rid,key,curpageid);
			if (status==DONE)
			{
				cout<<"|| ";
				MINIBASE_BM->unpinPage(curindexid);
				continue;
			}
			else if (status!=OK)
			{
				cout<<"reveal error"<<endl;
				MINIBASE_BM->unpinPage(curindexid);
				return;
			}

			nextlevelpage.push_back(curpageid);
			if (type==attrString)
			{
				cout<<*i<<": |"<<key->charkey<<","<<curpageid;
			}
			if (type==attrInteger)
			{
				cout<<*i<<": |"<<key->intkey<<","<<curpageid;
			}
			do
			{
				key=new Keytype;
				status=curindex->get_next(rid,key,curpageid);
				if (status==NOMORERECS)
				{
					cout<<"| ";
					MINIBASE_BM->unpinPage(curindexid);
					break;
				}
				nextlevelpage.push_back(curpageid);
				if (type==attrString)
				{
					cout<<"|"<<key->charkey<<","<<curpageid;
				}
				if (type==attrInteger)
				{
					cout<<"|"<<key->intkey<<","<<curpageid;
				}
			} while (1);
		}
		curlevelpage.clear();
		curlevelpage=nextlevelpage;
		nextlevelpage.clear();
		cout<<endl;

	}while(1);
}

void BTreeFile::content(PageId testpageno)
{
	Status status;

	cout<<"content of pageno "<<testpageno<<endl;
	BTLeafPage* testpage=new BTLeafPage;
	MINIBASE_BM->pinPage(testpageno,(Page*&)testpage);
	RID rid,datarid;
	Keytype* key=new Keytype;
	testpage->get_first(rid,key,datarid);
	if (type==attrInteger)
	{
		cout<<"|"<<*((int*)key);
	}
	if (type==attrString)
	{
		cout<<"|"<<key->charkey;
	}
	do
	{
		status=testpage->get_next(rid,key,datarid);
		if (status!=OK)
		{
			cout<<"|"<<endl;
			return;
		}
		if (type==attrInteger)
		{
		cout<<"|"<<*((int*)key);
		}
		if (type==attrString)
		{
			cout<<"|"<<key->charkey;
		}
	}while(1);
	cout<<"|"<<endl;
	return;

}
