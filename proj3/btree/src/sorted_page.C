/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"
#include <cstring>

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid, int cover)
{
  // put your code here
	Status status;
	status=HFPage::insertRecord(recPtr,recLen,rid);
	if (status!=OK)
		return status;
	Keytype* curkey;
	int i;
	for (i =0; i!=slotCnt-1;++i)
	{
		if (slot[i].length==-1)
		{
			continue;
		}
		curkey=(Keytype*)(data+slot[i].offset);
		int result;
		result=keyCompare(curkey,recPtr,key_type);
		if (result>=0)
		{
			break;
		}
		if (result==0 && cover==0)
		{
			memcpy(data+slot[i].offset,recPtr,recLen);
			status=HFPage::deleteRecord(rid);
			return OK;
		}
	}
	if (i>=slotCnt-1)
	{
		return OK;
	}
	slot_t tempslot;
	tempslot=slot[slotCnt-1];

	for (int j = slotCnt-1; j!= i; j--)
	{
		slot[j]=slot[j-1];
	}
	slot[i]=tempslot;

	return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
  // put your code here
	Status status;
	status=HFPage::deleteRecord(rid);
	return status;
}

int SortedPage::numberOfRecords()
{
  // put your code here
	int cnt=0;
	for (int i=0; i != slotCnt; ++i)
	{
		if (slot[i].length!=-1)
		{
			cnt++;
		}
	}
  return cnt;
}

Status SortedPage::compress()
{
	if (usedPtr<=6*(sizeof(slot_t)))
	{
		cout<<"warning! data overlapped"<<endl;
		getchar();
	}
	int slotcnt=slotCnt;
	int i =0;
	int j =0;
	int cnt=0;
	for (i=0; i!=slotCnt;++i)
	{
		if (slot[i].length!=-1)
		{
			++cnt;
		}
		if (slot[i].length==-1)
		{
			for (j=i+1;j!=slotCnt;++j)
			{
				if (slot[j].length!=-1)
				{
					slot[i].offset=slot[j].offset;
					slot[i].length=slot[j].length;
					slot[j].offset=-1;
					slot[j].length=-1;
					++cnt;
					break;
				}

			}
		}
	}
	freeSpace=freeSpace+sizeof(int)*(slotcnt-cnt);
	slotCnt=cnt;

	return OK;
}

