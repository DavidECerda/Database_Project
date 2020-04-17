# OurSQL: The Database for the Proletariat
###### Database project by Matthew Ha, Harris Zia, and David Cerda
---

## Overview

The project was to implement a L-store database ([original paper on lstore](https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System) and [relevant slides](https://expolab.org/papers/l-store-slides.pdf)) and consisted of three milestones that built upon each other. 

## Milestone 1

### Goal
Create a single-threaded, in memory database, based on L-Store which has the capacity to perform simple SQL-like operations.

### Objectives
* Data Model: Columnar, where the data from different records for the same column are stored together. 
    * Base record: original version of a record inserted into the database
    * Tail record: update to base record
    * Base page: Read-only pages that store the base records 
    * Tail page: Append-only pages associated with a base page that hold corresponding tail records i.e. updates.
    * Schema encoding: An binary string that has a '1' for updated columns and '0' for columns not yet updated. Converted to an int for storage
    * RID and Indirection: An RID is unique identifier for each record and each record has an indirection column which points to the tail record containing the latest update. 
    * Pageranges: 
* Bufferpool Management
* Query Interface

## Milestone 2

## Milestone 3








Milestone 2 Summary and Writeup

Questions:
	Increasing TID okay?
  TPS questions?

1. Durability, Bufferpool Extension
	a. Caching pages
  	i. LRU? MRU?
    ii. eviction policy granularity (page_range, single page (standard practice), etc)
	b. Mem <-> Disk
  c. Have flag (is_in_memory) in Page class
  	i. dirty and pin flags
  d. read_pid -> PageManager.read()
  e. implement open() and close() in db.py

2. Make merge (contention free)
	before: multithreading in python (background thread)
	a. Implement TPS's (Tail page sequencing number)
  	b. TPS is last merge updated, so we can see which tails have not been merged
  b. Merge tail by tail, latest first
  c. Don't delete tail records, just want to make selects/read more efficient
  d. MergeJob class, returns Page object (not inserted into a PageRange yet)
  e. Make of the old base page for read/write access
  
  Merge Process
  	Notes:
  		1. 1 2 3 4 | 5
    	2. TPS -> 4
    	3. 2^64 - 1 -> 0
    1. Make copy of base page. 
    2. Find latest tail record, merge that and anything before it
    3. Change TPS to equal the latest merged record
    4. New merged page -> RID of original base record
    6. Check indirection the base page, and compare to our copy to see if updates happened during merge
    7. Update RecordPids.columns variable (rename RecordPids to MetaRecord maybe cause it's not as ugly)
    8. Change schema encoding of base record
    8. BONUS: After all read requests prior to merge have been processed, deallocate outdated base page
  
3. Indexing
	a. Create indexing system for other columnn
  b. Index class -> Singleton
  	i. key_index : user key : RID
    ii. Index.indices = [key_index, col1_index]
    	I. change key_index to an array
  c. First as a list, then as a binary tree