/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"
#include <string.h>
const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
	KeyDataEntry* dataentry=new KeyDataEntry;
	Datatype data;
	data.rid=dataRid;
	int* pentry_len=new int;
	make_entry(dataentry,key_type,key,LEAF,data,pentry_len);
	Status status;
	status=SortedPage::insertRecord(key_type,(char*)dataentry,*pentry_len,rid,0);
	if (status!=OK)
		return status;
  // put your code here
  return OK;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(const void *key,
                                AttrType key_type,
                                RID &dataRid)
{
  // put your code here
	RID rid;
	Keytype* curkey=new Keytype;
	RID datarid;
	Status status;
	status=get_first(rid,curkey,datarid);
	while (keyCompare(key,curkey,key_type)>=0)
	{
		if (keyCompare(key,curkey,key_type)==0)
		{
			dataRid=datarid;
			return OK;
		}
		status=get_next(rid,curkey,datarid);
		if (status==NOMORERECS)
		{
			return FAIL;
		}
		if (status!=OK)
			return status;
	}
	return DONE;

}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
	Status status;
	status=HFPage::firstRecord(rid);
	if (status!=OK)
	{
		cout<<"Error: BTLeafPage: get_first 1"<<endl;
		getchar();
		return status;
	}
	KeyDataEntry* dataentry;
	char* entryptr;
	int entry_len;
	status=HFPage::returnRecord(rid,entryptr,entry_len);
	if (status!=OK)
	{
		cout<<"Error: BTLeafPage: get_first 2"<<endl;
		return status;
	}
	dataentry=(KeyDataEntry*)entryptr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	get_key_data(keytype,datatype,dataentry,entry_len,LEAF);
	memcpy((char*)key,(char*)keytype,entry_len-sizeof(RID));
	dataRid=datatype->rid;
  return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
	RID nextRID;
	Status status;
	status=HFPage::nextRecord(rid,nextRID);
	if (status==DONE)
		return NOMORERECS;
	else if (status==FAIL)
	{
		return NOMORERECS;
	}
	rid=nextRID;
	KeyDataEntry* dataentry;
	char* entryptr;
	int entry_len;
	status=HFPage::returnRecord(rid,entryptr,entry_len);
	if (status!=OK)
	{
		cout<<"Error: BTLeafPage: get_next2"<<endl;
		return status;
	}
	dataentry=(KeyDataEntry*)entryptr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	get_key_data(keytype,datatype,dataentry,entry_len,LEAF);
	memcpy((char*)key,(char*)keytype,entry_len-sizeof(RID));
	dataRid=datatype->rid;
  return OK;

}
