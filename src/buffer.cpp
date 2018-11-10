/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	for(FrameId cur = 0; cur < numBufs; cur++) {
		if(bufDescTable[cur].valid == true && bufDescTable[cur].dirty == true) {
			bufDescTable[cur].file->writePage(bufPool[bufDescTable[cur].frameNo]);
		}
	}
	delete [] bufPool;
	delete [] bufDescTable;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	clockHand  = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame)
{
	FrameId numpinned = 0;
	FrameId origin = clockHand;
	bool done = false;
	advanceClock();
	while(!done && numpinned < numBufs) {
		if (clockHand == origin) {
			numpinned = 0;
		}
		if(bufDescTable[clockHand].valid == false) {
			frame = bufDescTable[clockHand].frameNo;
			done = true;
		}
		else if(bufDescTable[clockHand].refbit == true) {
			bufDescTable[clockHand].refbit = false;
			advanceClock();
		}
		else if(bufDescTable[clockHand].pinCnt > 0) {
			numpinned++;
			advanceClock();
		}
		else {
			if(bufDescTable[clockHand].dirty == true) {
				bufDescTable[clockHand].file->writePage(bufPool[bufDescTable[clockHand].frameNo]);
			}
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			frame = bufDescTable[clockHand].frameNo;
			bufDescTable[clockHand].Clear();
			done = true;
		}
	}
	if(!done) {
		throw BufferExceededException();
	}
}


void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frame;
	try {
		hashTable->lookup(file, pageNo, frame);
		bufDescTable[frame].refbit = true;
		bufDescTable[frame].pinCnt++;
		page = &bufPool[frame];
	}
	catch (HashNotFoundException e) {
		allocBuf(frame);
		bufPool[frame] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);
		page = &bufPool[frame];
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	FrameId frame;
	try {
		hashTable->lookup(file, pageNo, frame);
		if (bufDescTable[frame].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frame);
		}
		else {
			bufDescTable[frame].pinCnt--;
		}
		if(dirty == true) {
			bufDescTable[frame].dirty = true;
		}
	}
	catch(HashNotFoundException e) {
		// nothing done here
	}
}

void BufMgr::flushFile(const File* file)
{
	for(FrameId cur = 0; cur < numBufs; cur++) {
		if(bufDescTable[cur].file == file) {
			if(bufDescTable[cur].valid == false) {
				throw BadBufferException(cur, bufDescTable[cur].dirty, bufDescTable[cur].valid, bufDescTable[cur].refbit);
			}
			else {
				if(bufDescTable[cur].pinCnt != 0) {
					throw PagePinnedException(file->filename(), bufDescTable[cur].pageNo, cur);
				}
				if(bufDescTable[cur].dirty == true) {
					bufDescTable[cur].file->writePage(bufPool[cur]);
					bufDescTable[cur].dirty = false;
					hashTable->remove(file, bufDescTable[cur].pageNo);
					bufDescTable[cur].Clear();
				}
			}
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	FrameId frame;
	allocBuf(frame);
	bufPool[frame] = file->allocatePage();
	pageNo = bufPool[frame].page_number();
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
	page = &bufPool[frame];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frame;
	try {
		hashTable->lookup(file, PageNo, frame);
		hashTable->remove(file, PageNo);
		bufDescTable[frame].Clear();
		file->deletePage(PageNo);
	}
	catch(HashNotFoundException e) {
		// nothing done here
	}
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
