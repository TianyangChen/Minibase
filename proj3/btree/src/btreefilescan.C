/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
*/

BTreeFileScan::~BTreeFileScan()
{
  // put your code here
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
  // put your code here
	Status status;
	int reclen;
	if (currid.slotNo==-1)
	{
		currid.pageNo=curpageid;

		if (keytype==attrInteger)
		{
			status=curpage->get_first(currid,keyptr,rid);
			if (status!=OK)
			{
				return status;
			}
			if (keycompare(keyptr)==true)
			{
				return OK;
			}
		}
		if (keytype==attrString)
		{
			status=curpage->firstRecord(currid);
			if (status!=OK)
			{
				status=NOMORERECS;
			}
			if (status==OK)
			{
				char* recptr;
				curpage->returnRecord(currid,recptr,reclen);
				Keytype* keytype=new Keytype;
				Datatype* datatype=new Datatype;
				get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
				memcpy((char*)keyptr,recptr,reclen-sizeof(RID));
				rid=datatype->rid;
				if (keycompare(recptr)==true)
				{
					return OK;
				}
			}
		}
		do
		{
			if (keytype==attrInteger)
			{
				status=curpage->get_next(currid,keyptr,rid);
				if (status==FAIL)
				{
					return DONE;
				}
				if (status!=OK)
				{
					return DONE;
				}
				if (keycompare(keyptr)==true)
				{
					return OK;
				}
			}
			else if (keytype==attrString)
			{
				status=curpage->nextRecord(currid,currid);
				if (status!=OK)
				{
					status=NOMORERECS;
					return DONE;
				}
				Keytype* key5=new Keytype;

				if (status==OK)
				{
					char* recptr;
					curpage->returnRecord(currid,recptr,reclen);
					Keytype* keytype=new Keytype;
					Datatype* datatype=new Datatype;
					get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
					memcpy((char*)keyptr,recptr,reclen-sizeof(RID));
					rid=datatype->rid;
					memcpy((char*)key5,recptr,reclen-sizeof(RID));
				}
				if (keycompare(key5)==true)
				{
					return OK;
				}
			}
		}while(1);
	}
	if (keytype==attrInteger)
	{
		status=curpage->get_next(currid,keyptr,rid);
	}
	if (keytype==attrString)
	{
		status=curpage->nextRecord(currid,currid);

		if (status!=OK)
		{
			status=NOMORERECS;
		}
		if (status==OK)
		{
		char* recptr;

		curpage->returnRecord(currid,recptr,reclen);
		Keytype* keytype=new Keytype;
		Datatype* datatype=new Datatype;

		get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
		memcpy((char*)keyptr,recptr,reclen-sizeof(RID));
		rid=datatype->rid;
		}
	}

	if (keytype==attrString)
	{
		Keytype* key2=new Keytype;
		if (status==OK)
		{
		memcpy(key2,keyptr,reclen-sizeof(RID));
		}

		if (status==OK && keycompare(key2))
			{
				return OK;
			}
			if (status==OK && keycompare(key2)==false)
			{

				return DONE;
			}
	}
	if (keytype==attrInteger)
	{
		if (status==OK && keycompare(keyptr))
		{
			return OK;
		}
		if (status==OK && keycompare(keyptr)==false)
		{
			return DONE;
		}
	}
	if (status==NOMORERECS)
	{
		int nextpageid=curpage->getNextPage();
		if (nextpageid==-1)
		{
			return DONE;
		}
		MINIBASE_BM->unpinPage(curpageid);
		curpageid=nextpageid;
		MINIBASE_BM->pinPage(curpageid,(Page*&)curpage);
		if (keytype==attrInteger)
		{
			status=curpage->get_first(currid,keyptr,rid);
		}

		else if (keytype==attrString)
		{
			status=curpage->firstRecord(currid);
			if (status!=OK)
			{
				status=NOMORERECS;
			}
			if (status==OK)
			{
				char* recptr;
				curpage->returnRecord(currid,recptr,reclen);
				Keytype* keytype=new Keytype;
				Datatype* datatype=new Datatype;
				get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
				memcpy((char*)keyptr,recptr,reclen-sizeof(RID));
				rid=datatype->rid;
			}
		}
		if (status!=OK)
		{
			return status;
		}
		if (keytype==attrInteger)
		{
			if (keycompare(keyptr)==false)
			{
				return DONE;
			}
		}
		if (keytype==attrString)
		{
			Keytype* key2=new Keytype;
			memcpy(key2,keyptr,reclen-sizeof(RID));
			if (keycompare(key2)==false)
			{
				return DONE;
			}
		}
		return OK;
	}
  return FAIL;
}

Status BTreeFileScan::delete_current()
{
  // put your code here
	Status status;
	status=curpage->deleteRecord(currid);
	return status;
	return OK;
}


int BTreeFileScan::keysize() 
{
  // put your code here
  return keySize;
}

bool BTreeFileScan::keycompare(const void* keyptr)
{
	bool left,right;
	if (lokey==NULL)
	{
		left=true;
	}
	else
	{
		if (keyCompare(keyptr,lokey,keytype)>=0)
		{
			left=true;
		}
		else
		{
			left=false;
		}
	}
	if (hikey==NULL)
	{
		right=true;
	}
	else
	{
		if (keyCompare(keyptr,hikey,keytype)==0)
		{
			return true;
		}
		if (keyCompare(keyptr,hikey,keytype)<=0)
		{
			right=true;
		}
		else
		{
			right=false;
		}
	}
	if (left==true && right==true)
	{
		return true;
	}
	return false;
}
