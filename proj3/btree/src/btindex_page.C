/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include <string.h>
// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
	KeyDataEntry* dataentry=new KeyDataEntry;
	Datatype data;
	data.pageNo=pageNo;
	int* pentry_len=new int;
	make_entry(dataentry,key_type,key,INDEX,data,pentry_len);
	Status status;
	status=SortedPage::insertRecord(key_type,(char*)dataentry,*pentry_len,rid,1);
	if (status!=OK)
		return status;
  // put your code here
  return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
	RID rid;
	Keytype* curkey=new Keytype;
	PageId curpageno;
	Status status;
	status=get_first(rid,curkey,curpageno);
	while (keyCompare(key,curkey,key_type)>=0)
	{
		if (keyCompare(key,curkey,key_type)==0)
		{
			curRid=rid;
			status=SortedPage::deleteRecord(rid);
			if (status!=OK)
				return status;
			return OK;
		}
		status=get_next(rid,curkey,curpageno);
		if (status!=OK)
			return status;
	}
	return DONE;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
  // put your code here
	RID rid;
	Keytype* curkey=new Keytype;
	PageId dataid;
	Status status;
	status=get_first(rid,curkey,dataid);
	while (keyCompare(key,curkey,key_type)>=0)
	{
		if (keyCompare(key,curkey,key_type)==0)
		{
			pageNo=dataid;
			return OK;
		}
		status=get_next(rid,curkey,dataid);
		if (status==NOMORERECS)
		{
			return FAIL;
		}
		if (status!=OK)
			return status;
	}
	return DONE;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
  // put your code here
	Status status;
	status=HFPage::firstRecord(rid);
	if (status!=OK)
	{
		cout<<"Error: BTIndexPage: get_first1"<<endl;
		return status;
	}
	KeyDataEntry* dataentry;
	char* entryptr;
	int entry_len;
	status=HFPage::returnRecord(rid,entryptr,entry_len);
	if (status!=OK)
	{
		cout<<"Error: BTIndexPage: get_first2"<<endl;
		return status;
	}
	dataentry=(KeyDataEntry*)entryptr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	get_key_data(keytype,datatype,dataentry,entry_len,INDEX);
//	cout<<datatype->pageNo<<endl;
	memcpy((char*)key,(char*)keytype,entry_len-sizeof(PageId));
	pageNo=datatype->pageNo;
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
  // put your code here
	RID nextRID;
	Status status;

	status=HFPage::nextRecord(rid,nextRID);
	if (status==DONE)
		return NOMORERECS;
	else if (status==FAIL)
	{
		cout<<"Error: BTIndexPage: get_next1"<<endl;
		return FAIL;
	}
	rid=nextRID;
	KeyDataEntry* dataentry;
	char* entryptr;
	int entry_len;
	status=HFPage::returnRecord(rid,entryptr,entry_len);
	if (status!=OK)
	{
		cout<<"Error: BTIndexPage: get_next2"<<endl;
		return status;
	}
	dataentry=(KeyDataEntry*)entryptr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	get_key_data(keytype,datatype,dataentry,entry_len,INDEX);
	memcpy((char*)key,(char*)keytype,entry_len-sizeof(PageId));
	pageNo=datatype->pageNo;
  return OK;
}
