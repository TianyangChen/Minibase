/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  // put your code here
	if (t==attrString)
	{
		char* str1=(char*)key1;
		char* str2=(char*)key2;
		if (strcmp(str1,str2)<0)
			return -1;
		else if (strcmp(str1,str2)>0)
			return 1;
		else if (strcmp(str1,str2)==0)
			return 0;
		else
			return -2;
	}
	else if (t==attrInteger)
	{
		int* int1=(int*)key1;
		int* int2=(int*)key2;
		if (*int1<*int2)
			return -1;
		else if (*int1>*int2)
			return 1;
		else if (*int1==*int2)
			return 0;
		else
			return -2;
	}
	else
		return -2;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
  // put your code here

	if (key_type==attrString)
	{
		string str=(char*)key;
			memcpy((char*)target,(char*)key,strlen((char*)key));
		int startpos=str.length();
		if (ndtype==INDEX)
		{
			*((int*)(((char*)target)+startpos))=*((int*)&data);
			*pentry_len=startpos+sizeof(PageId);
		}
		else if (ndtype==LEAF)
		{
			*((int*)(((char*)target)+startpos))=*((int*)&data);
			*((int*)(((char*)target)+startpos+4))=data.rid.slotNo;
			*pentry_len=startpos+sizeof(RID);
		}
	}
	else if (key_type==attrInteger)
	{
		*(((int*)target))=*((int*)key);
		if (ndtype==INDEX)
		{

			*(((int*)target)+1)=data.pageNo;
			*pentry_len=sizeof(int)+sizeof(PageId);

		}
		else if (ndtype==LEAF)
		{
			*(((int*)target)+1)=*((int*)&data);
			*(((int*)target)+2)=*((int*)&data+1);
			*pentry_len=sizeof(int)+sizeof(RID);
		}
	}

  return;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
   // put your code here
	int data_pos;
	int data_len;
	if (ndtype==INDEX)
		data_len=sizeof(PageId);
	else if (ndtype==LEAF)
		data_len=sizeof(RID);
	data_pos=entry_len-data_len;
	if (data_pos==4)
	{
		*(((int*)targetkey))=*(((int*)psource));
	}
	else
	{
		string str=(char*)psource;
		memcpy((char*)targetkey,(char*)psource,data_pos);
	}
	*(((int*)targetdata))=*((int*)(((char*)psource)+data_pos));
		if (ndtype==LEAF)
	{
		*(((int*)targetdata)+1)=*((int*)(((char*)psource)+data_pos+4));
	}
   return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
 // put your code here
	if (key_type==attrInteger)
		return sizeof(int);
	else if (key_type==attrString)
	{
		string str=(char*)key;
		return strlen((char*)key);
	}
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
 // put your code here
	int length=0;
	if (ndtype==INDEX)
		length+=sizeof(PageId);
	else if (ndtype==LEAF)
		length+=sizeof(RID);
	if (key_type==attrInteger)
		length+=sizeof(int);
	else if (key_type==attrString)
	{
		string str=(char*)key;
		length+=strlen((char*)key);
	}

 return length;
}
