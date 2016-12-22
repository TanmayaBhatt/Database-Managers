
/********************************************************/
                   Record Manager: 
/********************************************************/

Record manager is an interface for creating, inserting, updating and deleting records from the Page File.
 
/********************************************************/
                      How to Run:
/********************************************************/

make assignment3  (to run default testcases)

make clean (to remove the executable file)

/********************************************************/
      Functionalitys provided in the Storage manager:
/********************************************************/

1.initRecordManager:
=====================
This functionality is used to initialize the record manager.

2.shutdownRecordManager:
=========================
This functionality is used to shutdown a record manager.

3.Create Table :
================
This functionality is used to create a table and the page file.

4.Open Table:
=============

This functionaity is used to open the  specified table.

5.Close Table:
==============

This functionality is used to close the table specified.

6.Delete Table:
===============
This functionality is used to delete the specified table from memory.

7.GetNumTuples:
===============
This functionality is used to get the number of tuples from the table.

8.Insert record:
================
This functionality is used to insert a record into the table.

9.Delete Record:
================
This functionality is used to delete a record from the table.

10.Update Record:
================
This functionality is used to update a record in a table

11.Get Record:
=============
This functionality is used to get a record from a table.

12.Start Scan:
==============
This functionality is used to scan the records that satisfy a given expression.

13.Next:
========
This functionality is used to get the next record from the scan.

14.Close Scan:
==============
This functionality is used to close the scan handle.

15.Get RecordSize:
==================
This functionality is used to get the record size from schema.

16.CreateSchema:
================
This functionality is used to create the schema from the given attributes.

17.Free Schema:
===============
This functionality is used to free the schema from the memory .

18.Create Record:
=================
This functionality is used to create record from the schema.

19.Free Record:
===============
This functionality is used to free the record .

20.GetAttr:
===========
This functionality is used to get the attribute value from the table.

21.Set attr:
============
This functionality is used to set the attribute value in a record.


22. RM_PageHeader :
=========================
It is a structure defined for page handling .

23.RM_TableMgmt:
================
It is a structure defined to hold the table related information.

24.RM_ScanMgmt:
===============
It is a structure defined to manage the table scan functionality.



/********************************************************/
               Files and their purpose:
/********************************************************/

1. Storage_mgr.c,Storage_mgr.h:
===============================
Files used from assignement 1 to provide storage manager functionality.


2.Buffer_mgr.c,Buffer_mgr.h,Buffer_mgr_stat.c,Buffer_mgr_stat.h: 
================================================================
Files from the assignment 2 which provides buffer manager functionality.

3. Record_mgr.c: 
=================
This file provides the functionalities of a record manager.

4. Record_mgr.h:
=================
It provides the  record manager interface.

5. dberror.h, dberror.c : 
=========================
They provide the error handling strategies for the record manager application.

6. test_assign3_1.c: 
====================
It provides test cases to test the record manager application.


/********************************************************/
               New error codes added:
/********************************************************/
1. RC_PAGE_NOT_FOUND_IN_CACHE 5
2. RC_FAIL_SHUTDOWN_POOL 7
3. RC_FAIL_FORCE_PAGE_DUETO_PIN_EXIT 8
4. RC_ALL_PAGE_RESOURCE_OCCUPIED -3


