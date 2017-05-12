
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};

static error_string_table ErrTable( JOINS, ErrMsgs );

// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         s               // Status of constructor
){

    //filr name of the sortedfile
	char sorted_filename1[10];
	char sorted_filename2[10];

    HeapFile* sorted_file1 = new HeapFile(sorted_filename1,s);
    HeapFile* sorted_file2 = new HeapFile (sorted_filename2,s);
    HeapFile* join_file = new HeapFile (filename3,s);

    //sort R and S
    int num_unpin_buffer = MINIBASE_BM -> getNumUnpinnedBuffers();
    Sort sort1(filename1, sorted_filename1, len_in1, in1, t1_str_sizes, join_col_in1, order, num_unpin_buffer, s);

    num_unpin_buffer = MINIBASE_BM -> getNumUnpinnedBuffers();
    Sort sort2(filename2, sorted_filename2, len_in2, in2, t2_str_sizes, join_col_in2, order, num_unpin_buffer, s);

    Scan* scan1=NULL;
    Scan* scan2=NULL;
    scan1 = sorted_file1->openScan(s);
    assert(s==OK);
    scan2 = sorted_file2->openScan(s);  
    assert(s==OK);

    //posite cursor to the first record
    RID rid1;
    RID rid2;
    s=scan1->mvNext(rid1);
    s=scan2->mvNext(rid2);

    void* t1;
    void* t2;
    t1= malloc(t1_str_sizes[join_col_in1]);
    t2= malloc(t2_str_sizes[join_col_in2]);

    //compute where the join column lies
    int begin_size1=0, begin_size2=0;
    for (int i = 0; i < join_col_in1; ++i)
    {
        begin_size1+=t1_str_sizes[i];
        /* code */
    }

    for (int i = 0; i < join_col_in2; ++i)
    {
        begin_size2+=t2_str_sizes[i];
        /* code */
    }

    int flag=0;
    RID markRid;
    while(1){
        char* recPtr1;
        char* recPtr2;
        int recLen1, recLen2;
        Status status;
       
        scan1 -> dataPage -> returnRecord(rid1, recPtr1, recLen1);
        scan2 -> dataPage -> returnRecord(rid2, recPtr2, recLen2);

        t1=memcpy(t1, recPtr1+begin_size1, t1_str_sizes[join_col_in1]);
        t2=memcpy(t2, recPtr2+begin_size2, t2_str_sizes[join_col_in2]);
        if (tupleCmp(t1, t2)>0)//t1>t2
        {
            status=scan2->mvNext(rid2);
            if (status!=OK)
            {
                break;//no more record in S, jump out of the loop
            }
        }
        else if (tupleCmp(t1, t2)<0)//t1<t2
            {
                status=scan1->mvNext(rid1);
                if (status!=OK)
                {
                    break;//no more record in R, jump out of the loop
                }
            }
            else{//t1==t2
                char* join_rec_ptr=NULL;
                RID outRid;
                if (flag==0)
                {
                    markRid=rid2;
                    flag=1;
                }

                //insert the join record
                int length=recLen1+recLen2;
                join_rec_ptr = new char[length];   
                memcpy(join_rec_ptr, recPtr1, recLen1);            
             	memcpy(join_rec_ptr+recLen1, recPtr2, recLen2);
                join_file->insertRecord(join_rec_ptr, recLen1+recLen2, outRid);

                //peek the next tuple of S
                //if it's the last tuple, posite the cursor back
                status=scan2->mvNext(rid2);
                if (status!=OK)
                {
                    scan2 -> position(markRid);
                    status=scan1->mvNext(rid1);
                    if (status!=OK)
                    {
                        break;
                    }
                }

                //peek the next tuple of S
                //if it exists, compare with R
                //if the value of S is greater than R, posite the cursor back
                scan2 -> dataPage -> returnRecord(rid2, recPtr2, recLen2);
                t2=memcpy(t2, recPtr2+begin_size2, t2_str_sizes[join_col_in2]);
                if (tupleCmp(t1, t2)<0)
                {
                    scan2 -> position(markRid);
                    rid2=markRid;
                    flag=0;
                    status=scan1->mvNext(rid1);
                    if (status!=OK)
                    {
                        break;//no more record in R, jump out of the loop
                    }
                }
            }
    }

	delete sorted_file1;
	delete sorted_file2;
    MINIBASE_DB -> delete_file_entry(sorted_filename1);
    MINIBASE_DB -> delete_file_entry(sorted_filename2);
    s=OK;
}

// sortMerge destructor
sortMerge::~sortMerge()
{

	// fill in the body
}
