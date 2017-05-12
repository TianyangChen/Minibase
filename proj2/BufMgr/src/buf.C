/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"


// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  // put your code here
  numBuffers=numbuf;
  hate_frame = new int[numBuffers];
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    hate_frame[i]=-1;
    /* code */
  }
  frmeTable = new FrameDesc[numBuffers];
  hashTable = new hash_table[HTSIZE];
  bufPool = new Page[numBuffers]; 
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if ((frmeTable+i)->dirty==TRUE)
    {
      flushPage((frmeTable+i)->pageNo);
      /* code */
    }
    /* code */
  }
  delete [] hate_frame;
  delete [] frmeTable;
  delete [] hashTable;
  delete [] bufPool;
  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  // put your code here
  Page* pg;
  pg = new Page;
  if (MINIBASE_DB->read_page(PageId_in_a_DB, pg) != OK)
  {
    return FAIL;
    /* code */
  }

  int hash = h(PageId_in_a_DB);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    int frameNo = (hashTable+hash) -> pair[i].frameNo;
    if (pageNo == PageId_in_a_DB) 
    {
      (frmeTable+frameNo) -> pin_cnt++;
      page = bufPool+frameNo;
      return OK;
      /* code */
    }
    /* code */
  }

  if (getNumUnpinnedBuffers()==0)
  {
    return FAIL;
    /* code */
  }
  unsigned int cnt=0;
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if ((frmeTable+i) -> pageNo != -1)
    {
      cnt++;
      /* code */
    }
    /* code */
  }

  if (cnt==numBuffers)
  {
   
  int frameNo_replace;
  for (unsigned int i = numBuffers-1; i >= 0; i--)
  {
    if (hate_frame[i] != -1 && (frmeTable+hate_frame[i]) -> pin_cnt == 0)
    {
      
      frameNo_replace=hate_frame[i];
      break;
      /* code */
    }   
    /* code */
  }

  if ((frmeTable+frameNo_replace) -> dirty == TRUE)
  {
    flushPage((frmeTable+frameNo_replace) -> pageNo);
    /* code */
  }
  hash = h((frmeTable+frameNo_replace) -> pageNo);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
   
    if (pageNo == (frmeTable+frameNo_replace) -> pageNo) 
    {
      (hashTable+hash) -> pair[i].pageNo=-1;
      (hashTable+hash) -> pair[i].frameNo=-1;
      break;
      /* code */
    }
    /* code */
  }
  hash = h(PageId_in_a_DB);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    if ((hashTable+hash) -> pair[i].pageNo == -1)
    {
      (hashTable+hash) -> pair[i].pageNo = PageId_in_a_DB;
      (hashTable+hash) -> pair[i].frameNo = frameNo_replace;
      break;
      /* code */
    }
    /* code */
  }
  page = bufPool+frameNo_replace;
  if (emptyPage==FALSE)
  {
    MINIBASE_DB -> read_page(PageId_in_a_DB, page);
    /* code */
  }
  
  (frmeTable+frameNo_replace) -> pageNo=PageId_in_a_DB;
  (frmeTable+frameNo_replace) -> pin_cnt=0;
  (frmeTable+frameNo_replace) -> dirty=FALSE;
  (frmeTable+frameNo_replace) -> hate=0;

  (frmeTable+frameNo_replace) -> pin_cnt++;

    /* code */
  }else{
    int frameNo_free;
    for (unsigned int i = 0; i < numBuffers; ++i)
    {
    if ((frmeTable+i) -> pageNo == -1)
      {
        frameNo_free=i;
        break;
      /* code */
      }
    /* code */
    }
    hash = h(PageId_in_a_DB);
    for (unsigned int i = 0; i < numBuffers; i++)
    {
      if ((hashTable+hash) -> pair[i].pageNo == -1)
      {
        (hashTable+hash) -> pair[i].pageNo = PageId_in_a_DB;
        (hashTable+hash) -> pair[i].frameNo = frameNo_free;
        break;
      /* code */
      }
    /* code */
    }
    page = bufPool+frameNo_free;
    if (emptyPage==FALSE)
    {
      MINIBASE_DB -> read_page(PageId_in_a_DB, page);
      /* code */
    }
    
    (frmeTable+frameNo_free) -> pageNo=PageId_in_a_DB;
    (frmeTable+frameNo_free) -> pin_cnt=0;
    (frmeTable+frameNo_free) -> dirty=FALSE;
    (frmeTable+frameNo_free) -> hate=0;

    (frmeTable+frameNo_free) -> pin_cnt++;
  }
  
  return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty, int hate ){
  Page *pg;
  pg= new Page;
  if (MINIBASE_DB->read_page(page_num, pg) != OK)
  {
    return FAIL;
    /* code */
  }
  int hash = h(page_num);
  int frameNo_unpin;
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    int frameNo = (hashTable+hash) -> pair[i].frameNo;
    if (pageNo == page_num) 
    {
      frameNo_unpin=frameNo;
      break;
      /* code */
    }
    /* code */
  }

  if ((frmeTable+frameNo_unpin) -> pin_cnt==0)
  {
    return FAIL;
    /* code */
  }else{
    (frmeTable+frameNo_unpin) -> pin_cnt--;
    if ((frmeTable+frameNo_unpin) -> pin_cnt<0)
    {
      (frmeTable+frameNo_unpin) -> pin_cnt=0;
    }
  }
  if ((frmeTable+frameNo_unpin) -> dirty==TRUE)
  {

  }else
  {
    (frmeTable+frameNo_unpin) -> dirty = dirty;
  }

  int hate_frame_empty=0;
  int hate_frame_duplicate=-1;
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if (hate_frame[i]==-1)
    {
      hate_frame_empty++;
      /* code */
    }
     /* code */
  }

  if (hate_frame_empty!=0)
  {
    int last_not_empty=0;
    for (unsigned int i = 0; i < numBuffers; ++i)
    {
      if (hate_frame[i] != -1)
      {
        last_not_empty++;
        /* code */
      }
      /* code */
    }

    for (int i = 0; i < last_not_empty; ++i)
    {
      if (hate_frame[i]==frameNo_unpin)
      {
        hate_frame_duplicate=i;
        break;
        /* code */
      }
      /* code */
    }
    if (hate_frame_duplicate != -1)
    {
      if (hate==TRUE)
      {
        if ((frmeTable+frameNo_unpin) -> hate == 1)
        {
          int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
          /* code */
        }else{
          int temp = hate_frame[hate_frame_duplicate];
          for (unsigned int i = hate_frame_duplicate; i < numBuffers-1; i++)
          {
            hate_frame[i]=hate_frame[i+1];
            /* code */
          }
          hate_frame[numBuffers-1]=temp;
        }
        /* code */
      }else{
        (frmeTable+frameNo_unpin) -> hate = 1;
        int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
      }
      /* code */
    }else{
      
      if (hate==FALSE)
      {
        (frmeTable+frameNo_unpin) -> hate = 1;
        for (int i = last_not_empty; i > 0; i--)
        {
          hate_frame[i]=hate_frame[i-1];
          /* code */
        }
        hate_frame[0]=frameNo_unpin;
        /* code */
      }else{
        hate_frame[last_not_empty]=frameNo_unpin;
      }
    }
    /* code */
  }else{

      for (unsigned int i = 0; i < numBuffers; ++i)
      {
        if (hate_frame[i]==frameNo_unpin)
        {
          hate_frame_duplicate=i;
          break;
          /* code */
        }
        /* code */
      }
      if (hate==TRUE)
      {
        if ((frmeTable+frameNo_unpin) -> hate == 1)
        {
          int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
          /* code */
        }else{
          int temp = hate_frame[hate_frame_duplicate];
          for (unsigned int i = hate_frame_duplicate; i < numBuffers-1; i++)
          {
            hate_frame[i]=hate_frame[i+1];
            /* code */
          }
          hate_frame[numBuffers-1]=temp;
        }
        /* code */
      }else{
        (frmeTable+frameNo_unpin) -> hate = 1;
        int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
      }
  }


  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
  unsigned int cnt=0;
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    if ((frmeTable+i) -> pin_cnt != 0)
    {
      cnt++;
      /* code */
    }
    /* code */
  }
  MINIBASE_DB -> allocate_page(firstPageId);
  if (cnt==numBuffers)
  {

    MINIBASE_DB -> deallocate_page(firstPageId);
    return FAIL;
    /* code */
  }else{
    pinPage(firstPageId, firstpage);
  }
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
  int hash = h(globalPageId);
  int frameNo;
  int pair_index;
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    if (pageNo == globalPageId) 
    {
      frameNo=(hashTable+hash) -> pair[i].frameNo;
      pair_index=i;
      break;
      /* code */
    }

    /* code */
  }
  if ((frmeTable+frameNo) -> pin_cnt==0)
  {
    (frmeTable+frameNo) -> pageNo=-1;
    (frmeTable+frameNo) -> dirty=FALSE;
    (frmeTable+frameNo) -> hate=0;
    ((hashTable+hash) -> pair[pair_index].pageNo)=-1;
    ((hashTable+hash) -> pair[pair_index].frameNo)=-1;
    MINIBASE_DB -> deallocate_page(globalPageId);
    /* code */
  }else{
    return FAIL;
  }

  

  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
  int hash = h(pageid);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    int frameNo = (hashTable+hash) -> pair[i].frameNo;
    if (pageNo == pageid) 
    {
      (frmeTable+frameNo) -> dirty=FALSE;
      MINIBASE_DB -> write_page(pageid, bufPool+frameNo);
      break;
      /* code */
    }

    /* code */
  }

  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  //put your code here
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if ((frmeTable+i) -> pageNo != -1)
    {
      flushPage((frmeTable+i) -> pageNo);
      /* code */
    }
    /* code */
  }
  return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
// put your code here
  Page* pg;
  pg = new Page;
  if (MINIBASE_DB->read_page(PageId_in_a_DB, pg) != OK)
  {
    return FAIL;
    /* code */
  }
  int hash = h(PageId_in_a_DB);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    int frameNo = (hashTable+hash) -> pair[i].frameNo;
    if (pageNo == PageId_in_a_DB) 
    {
      (frmeTable+frameNo) -> pin_cnt++;
      page = bufPool+frameNo;
      return OK;
      /* code */
    }
    /* code */
  }

  if (getNumUnpinnedBuffers()==0)
  {
    return FAIL;
    /* code */
  }
  unsigned int cnt=0;
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if ((frmeTable+i) -> pageNo != -1)
    {
      cnt++;
      /* code */
    }
    /* code */
  }

  if (cnt==numBuffers)
  {
   
  int frameNo_replace;
  for (unsigned int i = numBuffers-1; i >= 0; i--)
  {
    if (hate_frame[i] != -1 && (frmeTable+hate_frame[i]) -> pin_cnt == 0)
    {
      
      frameNo_replace=hate_frame[i];
      break;
      /* code */
    }   
    /* code */
  }

  if ((frmeTable+frameNo_replace) -> dirty == TRUE)
  {
    flushPage((frmeTable+frameNo_replace) -> pageNo);
    /* code */
  }
  hash = h((frmeTable+frameNo_replace) -> pageNo);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
   
    if (pageNo == (frmeTable+frameNo_replace) -> pageNo) 
    {
      (hashTable+hash) -> pair[i].pageNo=-1;
      (hashTable+hash) -> pair[i].frameNo=-1;
      break;
      /* code */
    }
    /* code */
  }
  hash = h(PageId_in_a_DB);
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    if ((hashTable+hash) -> pair[i].pageNo == -1)
    {
      (hashTable+hash) -> pair[i].pageNo = PageId_in_a_DB;
      (hashTable+hash) -> pair[i].frameNo = frameNo_replace;
      break;
      /* code */
    }
    /* code */
  }
  page = bufPool+frameNo_replace;
  if (emptyPage==FALSE)
  {
    MINIBASE_DB -> read_page(PageId_in_a_DB, page);
    /* code */
  }
  
  (frmeTable+frameNo_replace) -> pageNo=PageId_in_a_DB;
  (frmeTable+frameNo_replace) -> pin_cnt=0;
  (frmeTable+frameNo_replace) -> dirty=FALSE;
  (frmeTable+frameNo_replace) -> hate=0;

  (frmeTable+frameNo_replace) -> pin_cnt++;

    /* code */
  }else{
    int frameNo_free;
    for (unsigned int i = 0; i < numBuffers; ++i)
    {
    if ((frmeTable+i) -> pageNo == -1)
      {
        frameNo_free=i;
        break;
      /* code */
      }
    /* code */
    }
    hash = h(PageId_in_a_DB);
    for (unsigned int i = 0; i < numBuffers; i++)
    {
      if ((hashTable+hash) -> pair[i].pageNo == -1)
      {
        (hashTable+hash) -> pair[i].pageNo = PageId_in_a_DB;
        (hashTable+hash) -> pair[i].frameNo = frameNo_free;
        break;
      /* code */
      }
    /* code */
    }
    page = bufPool+frameNo_free;
    if (emptyPage==FALSE)
    {
      MINIBASE_DB -> read_page(PageId_in_a_DB, page);
      /* code */
    }
    
    (frmeTable+frameNo_free) -> pageNo=PageId_in_a_DB;
    (frmeTable+frameNo_free) -> pin_cnt=0;
    (frmeTable+frameNo_free) -> dirty=FALSE;
    (frmeTable+frameNo_free) -> hate=0;

    (frmeTable+frameNo_free) -> pin_cnt++;
  }
  
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  int hate=FALSE;
  int page_num=globalPageId_in_a_DB;
  Page *pg;
  pg= new Page;
  if (MINIBASE_DB->read_page(page_num, pg) != OK)
  {
    return FAIL;
    /* code */
  }
  int hash = h(page_num);
  int frameNo_unpin;
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    int pageNo = (hashTable+hash) -> pair[i].pageNo;
    int frameNo = (hashTable+hash) -> pair[i].frameNo;
    if (pageNo == page_num) 
    {
      frameNo_unpin=frameNo;
      break;
      /* code */
    }
    /* code */
  }

  if ((frmeTable+frameNo_unpin) -> pin_cnt==0)
  {
    return FAIL;
    /* code */
  }else{
    (frmeTable+frameNo_unpin) -> pin_cnt--;
    if ((frmeTable+frameNo_unpin) -> pin_cnt<0)
    {
      (frmeTable+frameNo_unpin) -> pin_cnt=0;
    }
  }
  if ((frmeTable+frameNo_unpin) -> dirty==TRUE)
  {

    /* code */
  }else
  {
    (frmeTable+frameNo_unpin) -> dirty = dirty;
  }

  int hate_frame_empty=0;
  int hate_frame_duplicate=-1;
  for (unsigned int i = 0; i < numBuffers; ++i)
  {
    if (hate_frame[i]==-1)
    {
      hate_frame_empty++;
      /* code */
    }
     /* code */
  }

  if (hate_frame_empty!=0)
  {
    int last_not_empty=0;
    for (unsigned int i = 0; i < numBuffers; ++i)
    {
      if (hate_frame[i] != -1)
      {
        last_not_empty++;
        /* code */
      }
      /* code */
    }

    for (int i = 0; i < last_not_empty; ++i)
    {
      if (hate_frame[i]==frameNo_unpin)
      {
        hate_frame_duplicate=i;
        break;
        /* code */
      }
      /* code */
    }
    if (hate_frame_duplicate != -1)
    {
      if (hate==TRUE)
      {
        if ((frmeTable+frameNo_unpin) -> hate == 1)
        {
          int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
          /* code */
        }else{
          int temp = hate_frame[hate_frame_duplicate];
          for (unsigned int i = hate_frame_duplicate; i < numBuffers-1; i++)
          {
            hate_frame[i]=hate_frame[i+1];
            /* code */
          }
          hate_frame[numBuffers-1]=temp;
        }
        /* code */
      }else{
        (frmeTable+frameNo_unpin) -> hate = 1;
        int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
      }
      /* code */
    }else{
      
      if (hate==FALSE)
      {
        (frmeTable+frameNo_unpin) -> hate = 1;
        for (int i = last_not_empty; i > 0; i--)
        {
          hate_frame[i]=hate_frame[i-1];
          /* code */
        }
        hate_frame[0]=frameNo_unpin;
        /* code */
      }else{
        hate_frame[last_not_empty]=frameNo_unpin;
      }
    }
    /* code */
  }else{

      for (unsigned int i = 0; i < numBuffers; ++i)
      {
        if (hate_frame[i]==frameNo_unpin)
        {
          hate_frame_duplicate=i;
          break;
          /* code */
        }
        /* code */
      }
      if (hate==TRUE)
      {
        if ((frmeTable+frameNo_unpin) -> hate == 1)
        {
          int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
          /* code */
        }else{
          int temp = hate_frame[hate_frame_duplicate];
          for (unsigned int i = hate_frame_duplicate; i < numBuffers-1; i++)
          {
            hate_frame[i]=hate_frame[i+1];
            /* code */
          }
          hate_frame[numBuffers-1]=temp;
        }
        /* code */
      }else{
        (frmeTable+frameNo_unpin) -> hate = 1;
        int temp = hate_frame[hate_frame_duplicate];
          for (int i = hate_frame_duplicate; i > 0; i--)
          {
            hate_frame[i]=hate_frame[i-1];
            /* code */
          }
          hate_frame[0]=temp;
      }
  }


  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
  int unpinnedbuffers_cnt=0;
  for (unsigned int i = 0; i < numBuffers; i++)
  {
    if ((frmeTable+i) -> pin_cnt == 0)
    {
      unpinnedbuffers_cnt++;
      /* code */
    }
    /* code */
  }
  return unpinnedbuffers_cnt;
}
