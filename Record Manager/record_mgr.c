#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

int startPagePosition;// Starting position of the record in table
int totalPageNo; // Total number of pages of the blocks loaded

typedef struct RM_SlotInfo {
	 
	char * availableSlot; // Pointer to the avaiable slot space
	
}RM_SlotInfo;


typedef struct RM_ScaningRecordInfo {
	Expr *condition; //Search condition
	int pageCurrentPosition; //Current page index
	int slotCurrentPosition; // Current slot index
	
}RM_ScaningRecordInfo;


typedef struct RM_IndexTableInfo {
	BM_BufferPool *bufferPoolForRecord; // Pointer of the BM_BufferPool to load disk block into memory page frame
	BM_PageHandle * pageHandleForList; // Pointer to the page frame managed by buffer pool
	
	
	 
}RM_IndexTableInfo;



RC initRecordManager(void *mgmtData){
	initStorageManager();
	return RC_OK;
}


RC shutdownRecordManager(){
	return RC_OK;
}


RC createTable(char *name, Schema *schema){
	SM_PageHandle ph;
	int i;
    SM_FileHandle fh;
	createPageFile(name);//Creates page file with the given name
    openPageFile(name, &fh);
	ensureCapacity(32, &fh); // ensure the table to fit in memory
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	
	int j;
	char *str;
	char *temp=ph;
	memcpy((char *)ph, &(schema->numAttr),sizeof(int));
	temp= (char *)ph + sizeof(int);
	
	for (i=0; i<schema->numAttr; i++){
		
		j = strlen(schema->attrNames[i]);
		memcpy(temp, &j, sizeof(int));
		temp= temp+ sizeof(int);
		}
	for (i=0; i<schema->numAttr; i++){
		memcpy(temp, schema->attrNames[i], strlen(schema->attrNames[i]));
		str = (char *)malloc(strlen(schema->attrNames[i]));
		strncpy(str, temp, strlen(schema->attrNames[i]));
		temp= temp+ strlen(schema->attrNames[i]);
		
		memcpy(temp, &(schema->dataTypes[i]),sizeof(DataType));
		temp= temp+ sizeof(DataType);
		memcpy(temp, &(schema->typeLength[i]),sizeof(int));
		temp= temp+ sizeof(int);
		}
	memcpy(temp, &(schema->keySize), sizeof(int));
	temp= temp+sizeof(int);
	for(i = 0; i < schema->keySize; i++){
		memcpy(temp, &(schema->keyAttrs[i]), sizeof(int)); 
		temp= temp+ sizeof(int);
	}
	writeBlock(0, &fh, ph);
	closePageFile(&fh); 
}



RC openTable(RM_TableData *rel, char *name){
	
	int pageIndex, maxSlot, usedSlot=0, slotSize;
	rel->schema = (Schema *)malloc(sizeof(Schema));
	rel->name = name;
	BM_BufferPool *bufferPoolForRecord = MAKE_POOL();
    initBufferPool(bufferPoolForRecord, name, 35, RS_FIFO, NULL);
	RM_IndexTableInfo *mgmt = ((RM_IndexTableInfo *)malloc (sizeof(RM_IndexTableInfo)))	;
	
	startPagePosition = -1;

	totalPageNo = 0;
	mgmt->bufferPoolForRecord = bufferPoolForRecord;
	rel->mgmtData = mgmt;
	SM_FileHandle fhTable;
	openPageFile(name, &fhTable);
	totalPageNo = fhTable.totalNumPages;
	closePageFile(&fhTable);
	
	BM_PageHandle *pageHandleForList = (BM_PageHandle *) malloc(sizeof(BM_PageHandle) * totalPageNo);
	mgmt->pageHandleForList= pageHandleForList;

	if (totalPageNo > 1){
		
		startPagePosition=-1;
	}
	
	 
	BM_PageHandle * page;
	
	
	for (pageIndex=0; pageIndex< totalPageNo; pageIndex++){

		page = &(mgmt->pageHandleForList[pageIndex]);
		pinPage(mgmt->bufferPoolForRecord, page, pageIndex);
		if(pageIndex == 0){
			
	int i,j=0,length,keySize;
	char *temp=page->data;
	
	memcpy(&(length), (char *)page->data, sizeof(int));

	char **cpNames = (char **) malloc(sizeof(char*) * length);
	char *str;
	temp= (char *)page->data+sizeof(int);
	int *nameSizes = (int *) malloc(sizeof(int) * length);
	DataType *cpDt = (DataType *) malloc(sizeof(DataType) * length);
	int *cpSizes = (int *) malloc(sizeof(int) * length);
	
	for (i=0; i<length; i++){
		memcpy(&j, temp	, sizeof(int));
		nameSizes[i] = j;
		temp = temp	 + sizeof(int);
	}
	for(i = 0; i < length; i++){
      cpNames[i] = (char *) malloc(nameSizes[i]+1);
      strncpy(cpNames[i], temp, nameSizes[i]);
	  temp= temp+nameSizes[i];
	  memcpy(&cpDt[i],temp, sizeof(DataType));
	  temp= temp+sizeof(DataType);
	  memcpy(&cpSizes[i],temp, sizeof(int));
	  temp= temp+sizeof(int);
    }
	memcpy(&(keySize), temp	, sizeof(int));
	int *cpKeys = (int *) malloc(sizeof(int)*keySize);
	temp= temp+sizeof(int);
	for(i=0; i<keySize; i++){
		memcpy(&(cpKeys[i]), temp, sizeof(int)); 
		temp= temp+ sizeof(int);
	}
	// Assign back to the schema pointer
	rel->schema->numAttr = length;
    rel->schema->attrNames = cpNames;
    rel->schema->dataTypes = cpDt;
    rel->schema->typeLength = cpSizes;
    rel->schema->keyAttrs = cpKeys;
    rel->schema->keySize = keySize;
			
	
		} else {
			int slotSize;
			slotSize = getRecordSize(rel->schema);
			int pageIndex, maxSlot, usedSlot=0;
			memcpy(&maxSlot,page->data, sizeof(int));
			if(maxSlot == 0){
				maxSlot=(PAGE_SIZE-sizeof(RM_SlotInfo))/slotSize;
				memcpy(page->data, &maxSlot, sizeof(int));
				memcpy(page->data+sizeof(int), &usedSlot, sizeof(int)); // write back
			} 
		}
	}
	
	return RC_OK;
}





RC closeTable(RM_TableData *rel){
	
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	int i;
	
	for (i=0; i<totalPageNo; i++){
		// write the memory data into the records in the disk 
		unpinPage(mgmt->bufferPoolForRecord, &(mgmt->pageHandleForList[i]));
		forcePage(mgmt->bufferPoolForRecord, &(mgmt->pageHandleForList[i]));
	}
	shutdownBufferPool(mgmt->bufferPoolForRecord); //shutdown the buffer pool
	free(mgmt->pageHandleForList); // Free allocation space
	free(mgmt->bufferPoolForRecord); // Free allocation space
	
	mgmt->bufferPoolForRecord = NULL;
	mgmt->pageHandleForList = NULL;
	
	free(rel->mgmtData);
	rel->mgmtData = NULL;
    freeSchema(rel->schema); //Free schema allocation space
	rel->schema = NULL;
    return RC_OK;
	
}


RC deleteTable (char *name){
	return destroyPageFile(name);	
}



int getNumTuples (RM_TableData *rel){
	int i,totTuple = 0;
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	BM_PageHandle * page;
	
	for (i=1; i<totalPageNo; i++){
		page = &(mgmt->pageHandleForList[i]);
		int tupleperpage;
		memcpy(&tupleperpage, page->data+sizeof(int), sizeof(int));
		totTuple += tupleperpage;
	}
	return totTuple;
}



RC insertRecord (RM_TableData *rel, Record *record){
		
	int i, usedSlot;
	int slot_size = getRecordSize(rel->schema);
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	int maxSlot = (PAGE_SIZE-sizeof(RM_SlotInfo))/getRecordSize(rel->schema);
	
	 
	BM_PageHandle * page;
	for (i=1; i<totalPageNo; i++){
		page = &(mgmt->pageHandleForList[i]);
	    memcpy(&usedSlot,page->data+sizeof(int), sizeof(int));
		if((maxSlot-usedSlot)> 0){
			memcpy(page->data+sizeof(RM_SlotInfo)+usedSlot*getRecordSize(rel->schema), record->data, getRecordSize(rel->schema));
			record->id.slot = usedSlot;
			record->id.page = page->pageNum;
			usedSlot ++;	
        	memcpy(page->data+sizeof(int), &usedSlot, sizeof(int));		   
			return RC_OK;
		} 
			
	}

}



RC deleteRecord (RM_TableData *rel, RID id){
	int i;
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	BM_PageHandle * page;
	for ( i=0; i<totalPageNo; i++){
		page = &(mgmt->pageHandleForList[i]);
		if(page->pageNum == id.page){
			memset(page->data+sizeof(RM_SlotInfo)+id.slot*getRecordSize(rel->schema), '\0', getRecordSize(rel->schema));
		}
	}
	
	return RC_OK;
	
}



RC updateRecord (RM_TableData *rel, Record *record){
	int i;
	BM_PageHandle * page;
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	RID id = record->id;
	
	
	for (i=0; i<totalPageNo; i++){
		page = &(mgmt->pageHandleForList[i]);
		if(page->pageNum == id.page){
			memcpy(page->data+sizeof(RM_SlotInfo)+id.slot*getRecordSize(rel->schema), record->data, getRecordSize(rel->schema));
		}
	}
	return RC_OK;
		
	
}



RC getRecord (RM_TableData *rel, RID id, Record *record){
	BM_PageHandle * page; 
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)rel->mgmtData;
	record->id = id;
	int i;
	for (i=0; i<totalPageNo; i++){
		page = &(mgmt->pageHandleForList[i]);
		if(page->pageNum == id.page){
			memcpy(record->data,page->data+sizeof(RM_SlotInfo)+id.slot*getRecordSize(rel->schema),getRecordSize(rel->schema));
			
		}
		
	}
	return RC_OK;

}


RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *condition){
	
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)malloc(sizeof(RM_ScaningRecordInfo));
	scan->rel = rel;
	scan->mgmtData= scan_mgmt;
	scan_mgmt->pageCurrentPosition = 1;
	scan_mgmt->slotCurrentPosition = 0;
	scan_mgmt->condition = condition;
	
	
	return RC_OK;
	
}


RC next(RM_ScanHandle *scan, Record *record){
	
	RM_IndexTableInfo *mgmt = (RM_IndexTableInfo *)scan->rel->mgmtData;
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)scan->mgmtData;
	
	Value *value; 
	BM_PageHandle * page;
	int usedSlot;
	
	record->id.page = scan_mgmt->pageCurrentPosition;
	record->id.slot = scan_mgmt->slotCurrentPosition;
	
	page = &(mgmt->pageHandleForList[scan_mgmt->pageCurrentPosition]);
	memcpy(&usedSlot, page->data+sizeof(int), sizeof(int));
	getRecord(scan->rel, record->id, record); //Get the record information
	evalExpr(record, scan->rel->schema, scan_mgmt->condition, &value); //Evaluate the record information
	
	if(usedSlot == record->id.slot)
	{
		if (scan_mgmt->pageCurrentPosition == (totalPageNo-1)){
			return RC_RM_NO_MORE_TUPLES;	
		}else {	
		scan_mgmt->slotCurrentPosition = 0; 
		scan_mgmt->pageCurrentPosition++;
		
		}
	} else {
		scan_mgmt->slotCurrentPosition++;
	}
	
	if (value->v.boolV != 1){
	   return next(scan,record);
	} else {
		
		return RC_OK;
	}

}
                   
RC closeScan (RM_ScanHandle *scan){
	
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)scan->mgmtData;
	free(scan_mgmt);
	scan->mgmtData = NULL;
	return RC_OK;
}



int getRecordSize(Schema *schema){
	int recordSize = 0;
	int i;
	for (i=0; i<schema->numAttr; i++){
		
		if(schema->dataTypes[i]==DT_STRING)
			{
					recordSize += schema->typeLength[i];
			}
			else 
			{
				if(schema->dataTypes[i]==DT_INT)
					{
					recordSize += sizeof(int);
					}
				else
				{
				if(schema->dataTypes[i]==DT_FLOAT)
				{
					recordSize +=  sizeof(float);
				}
				else
				{
					recordSize += sizeof(bool);
				}
				 }
			
			}		
	
	}
	return recordSize;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {   
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->attrNames = attrNames;
   	schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}



RC freeSchema(Schema *schema) {
	 free(schema->typeLength);
	  free(schema->keyAttrs);
	  free(schema->dataTypes);
	int i;
	for (i=0; i<schema->numAttr ; i++){
		free(schema->attrNames[i]);
	}
	/* Free all the allocated memory with respect to schema pointer */
    free(schema->attrNames);
    
    free(schema);
    return RC_OK;
}



RC createRecord(Record **record, Schema *schema){
	*record = (Record *)malloc(sizeof(Record));
	(*record)->data = (char *)malloc(sizeof(char)*getRecordSize(schema));
	return RC_OK;
}


RC freeRecord(Record *record){
	free(record->data);
	free(record);
	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value){
	bool tempb;
	int i, tempint, len,offset = 0;
	float tempf;
	char * str	;
	for (i=0; i<schema->numAttr; i++){
		if (i == attrNum){
		
			if(schema->dataTypes[i]==DT_STRING)
			{
					len = schema->typeLength[i];
					str	 = (char *)malloc(len);
					memcpy(str, (record->data+offset), len);
					MAKE_STRING_VALUE(*value,str);
			}
			else 
			{
				if(schema->dataTypes[i]==DT_INT)
					{
					memcpy(&tempint,(record->data+offset),sizeof(int));
					MAKE_VALUE(*value, schema->dataTypes[i], tempint);
					}
				else
				{
				if(schema->dataTypes[i]==DT_FLOAT)
				{
					memcpy(&tempf,(record->data+offset),sizeof(float));
					MAKE_VALUE(*value, schema->dataTypes[i], tempf);
				}
				else
				{
					memcpy(&tempb,(record->data+offset),sizeof(bool));
					MAKE_VALUE(*value, schema->dataTypes[i], tempb);
				}
				 }
			
			}		
		} else {
			
				if(schema->dataTypes[i]==DT_STRING)
			{
					offset += schema->typeLength[i];
			}
			else 
			{
				if(schema->dataTypes[i]==DT_INT)
					{
						offset += sizeof(int);
					}
				else
				{
				if(schema->dataTypes[i]==DT_FLOAT)
				{
					offset +=  sizeof(float);
				}
				else
				{
					offset += sizeof(bool);
				}
				 }
			
			}
		
		}
	
	}
	return RC_OK;
}


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value){
	int i, offset = 0, len;
	for (i=0; i<schema->numAttr; i++){
		if (i == attrNum){
		
			if(schema->dataTypes[i]==DT_STRING)
			{
					len = schema->typeLength[i];
					memcpy((record->data+offset),value->v.stringV, len);
			}
			else 
			{
				if(schema->dataTypes[i]==DT_INT)
					{
					memcpy((record->data+offset),&(value->v.intV), sizeof(int));
					}
				else
				{
				if(schema->dataTypes[i]==DT_FLOAT)
				{
					memcpy((record->data+offset),&(value->v.floatV), sizeof(float));
				}
				else
				{
					memcpy((record->data+offset),&(value->v.boolV), sizeof(bool));
				}
				 }
			
			}		
		
		} else {
		
			if(schema->dataTypes[i]==DT_STRING)
			{
					offset += schema->typeLength[i];
			}
			else 
			{
				if(schema->dataTypes[i]==DT_INT)
					{
					offset += sizeof(int);
					}
				else
				{
				if(schema->dataTypes[i]==DT_FLOAT)
				{
					offset +=  sizeof(float);
				}
				else
				{
					offset += sizeof(bool);
				}
				 }
			
			}		
		
		}
	
	}
	return RC_OK;
}
