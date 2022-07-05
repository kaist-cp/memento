/*
 *
 */

#include <pthread.h>
#include <iostream>
#include <cstdlib>
#include <time.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include <sys/time.h>

#include "RecoverableLinkedListTracking.h"
#include "RecoverableLinkedListCapsules.h"

#include "Utilities.h"


#define ADD __sync_fetch_and_add
#define BASIC 1

// -------------------  LEF ----------------------------------
#define TOTAL_OPERATIONS 	32768
#define LOOKUPS_PERCENTAGE 	(1-INSERTS_PERCENTAGE-DELETES_PERCENTAGE)
#define INSERTS (int)(INSERTS_PERCENTAGE*TOTAL_OPERATIONS)
#define DELETES (int)(DELETES_PERCENTAGE*TOTAL_OPERATIONS)
#define LOOKUPS (int)(LOOKUPS_PERCENTAGE*TOTAL_OPERATIONS)
#define INSERTS_PER_THREAD (int)(INSERTS/numThreads)
#define DELETES_PER_THREAD (int)(DELETES/numThreads)
#define LOOKUPS_PER_THREAD (int)(LOOKUPS/numThreads)
#define OPERATIONS_PER_THREAD (int)(TOTAL_OPERATIONS/numThreads)
// -------------------  LEF - END-----------------------------

using namespace std;

pthread_t threads[MAX_THREADS];
int arguments[MAX_THREADS * PADDING];
int numThreads = 2; // default value
int timeForRecord = 5; // default value
volatile bool run = false;
volatile bool stop = false; 

RecoverableLinkedListTracking<int> recoverableLinkedListTracking;
int totalNumRecoverableLinkedListTrackingActions = 0;

RecoverableLinkedListCapsules<int> recoverableLinkedListCapsules;
int totalNumRecoverableLinkedListCapsulesActions = 0;


#if defined(PROFILING)
// Tracking
unsigned long totalNumRecoverableLinkedListTrackingActions_InsertsSuccessful = 0;
unsigned long totalNumRecoverableLinkedListTrackingActions_InsertsUnSuccessful = 0;
unsigned long totalNumRecoverableLinkedListTrackingActions_DeletesSuccessful = 0;
unsigned long totalNumRecoverableLinkedListTrackingActions_DeletesUnSuccessful = 0;
unsigned long totalNumRecoverableLinkedListTrackingActions_FindsSuccessful = 0;
unsigned long totalNumRecoverableLinkedListTrackingActions_FindsUnSuccessful = 0;

unsigned long totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringSearches;
unsigned long totalNumRecoverableLinkedListTrackingActions_numInsertOps;
unsigned long totalNumRecoverableLinkedListTrackingActions_numInsertAttempts;
unsigned long totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringInserts;
unsigned long totalNumRecoverableLinkedListTrackingActions_numDeleteOps;
unsigned long totalNumRecoverableLinkedListTrackingActions_numDeleteAttempts;
unsigned long totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringDeletes;         
unsigned long totalNumRecoverableLinkedListTrackingActions_numFindOps;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFindAttempts;
unsigned long totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringFinds;
unsigned long totalNumRecoverableLinkedListTrackingActions_numSearchBarrier1;
unsigned long totalNumRecoverableLinkedListTrackingActions_numSearchBarrier2;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFindBarrier;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFlush;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFlushLow;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFlushMedium;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFlushHigh;
unsigned long totalNumRecoverableLinkedListTrackingActions_numBarrier;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFence;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFlushHelp;
unsigned long totalNumRecoverableLinkedListTrackingActions_numBarrierHelp;
unsigned long totalNumRecoverableLinkedListTrackingActions_numFenceHelp;

// Capsules
unsigned long totalNumRecoverableLinkedListCapsulesActions_InsertsSuccessful = 0;
unsigned long totalNumRecoverableLinkedListCapsulesActions_InsertsUnSuccessful = 0;
unsigned long totalNumRecoverableLinkedListCapsulesActions_DeletesSuccessful = 0;
unsigned long totalNumRecoverableLinkedListCapsulesActions_DeletesUnSuccessful = 0;
unsigned long totalNumRecoverableLinkedListCapsulesActions_FindsSuccessful = 0;
unsigned long totalNumRecoverableLinkedListCapsulesActions_FindsUnSuccessful = 0;

unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchOps;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringSearches;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numInsertOps;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numInsertAttempts;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringInserts;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numDeleteOps;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numDeleteAttempts;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringDeletes;         
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFindOps;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringFinds;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier1;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier2;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier3;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier4;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier5;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFlush;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFlushLow;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFlushMedium;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFlushHigh;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numBarrier;
unsigned long totalNumRecoverableLinkedListCapsulesActions_numFence;
#endif

//==========================================================================

inline static void* startRoutineRecoverableLinkedListTracking(void* argsInput){

    int thread_id = *(int*)argsInput;
    threadPin(thread_id);

    long numMyOps=0;

	#if defined(PROFILING)
    unsigned long numMyInsertsSuccessful = 0, numMyInsertsUnSuccessful = 0;
    unsigned long numMyDeletesSuccessful = 0, numMyDeletesUnSuccessful = 0;
    unsigned long numMyFindsSuccessful = 0, numMyFindsUnSuccessful = 0;
	#endif    

    RecoverableLinkedListTracking<int>& list = recoverableLinkedListTracking;

    int key;
    unsigned int seed = time(NULL) + thread_id;
    long op;

    fastRandomSetSeed(seed);

    while (run == false) {          // busy-wait to start "simultaneously"
        MFENCE();
        pthread_yield();
    }

    while(!stop){

        numMyOps++;
        op = fastRandomRange(1, 100);
        key = fastRandomRange(1, KEY_RANGE);

		if (op <= 100*INSERTS_PERCENTAGE) {
		#if defined(PROFILING)
            RecoverableLinkedListTracking<int>::prof.numInsertOps++;
            if (list.Insert(key, thread_id) == true) {
                numMyInsertsSuccessful++;
            }
            else {
                numMyInsertsUnSuccessful++;
            }
		#else
            list.Insert(key, thread_id);
		#endif      
        }
        else if (op <= 100*INSERTS_PERCENTAGE+100*DELETES_PERCENTAGE) {
		#if defined(PROFILING)
            RecoverableLinkedListTracking<int>::prof.numDeleteOps++;
            if (list.Delete(key, thread_id) == true) {
                numMyDeletesSuccessful++;
            }
            else {
                numMyDeletesUnSuccessful++;
            }
		#else
            list.Delete(key, thread_id);
		#endif            
        }
        else {
		#if defined(PROFILING)
            RecoverableLinkedListTracking<int>::prof.numFindOps++;
            if (list.Find(key, thread_id) == true) {
                numMyFindsSuccessful++;
            }
            else {
                numMyFindsUnSuccessful++;
            }
		#else
            list.Find(key, thread_id);
		#endif
        }
    }

    ADD(&totalNumRecoverableLinkedListTrackingActions, numMyOps);
	#if defined(PROFILING)
    ADD(&totalNumRecoverableLinkedListTrackingActions_InsertsSuccessful, numMyInsertsSuccessful);
    ADD(&totalNumRecoverableLinkedListTrackingActions_InsertsUnSuccessful, numMyInsertsUnSuccessful);
    ADD(&totalNumRecoverableLinkedListTrackingActions_DeletesSuccessful, numMyDeletesSuccessful);
    ADD(&totalNumRecoverableLinkedListTrackingActions_DeletesUnSuccessful, numMyDeletesUnSuccessful);
    ADD(&totalNumRecoverableLinkedListTrackingActions_FindsSuccessful, numMyFindsSuccessful);
    ADD(&totalNumRecoverableLinkedListTrackingActions_FindsUnSuccessful, numMyFindsUnSuccessful);

    ADD(&totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringSearches, RecoverableLinkedListTracking<int>::prof.numNodesAccessedDuringSearches);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numInsertOps, RecoverableLinkedListTracking<int>::prof.numInsertOps);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numInsertAttempts, RecoverableLinkedListTracking<int>::prof.numInsertAttempts);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringInserts, RecoverableLinkedListTracking<int>::prof.numNodesAccessedDuringInserts);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numDeleteOps, RecoverableLinkedListTracking<int>::prof.numDeleteOps);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numDeleteAttempts, RecoverableLinkedListTracking<int>::prof.numDeleteAttempts);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringDeletes, RecoverableLinkedListTracking<int>::prof.numNodesAccessedDuringDeletes); 
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFindOps, RecoverableLinkedListTracking<int>::prof.numFindOps);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFindAttempts, RecoverableLinkedListTracking<int>::prof.numFindAttempts);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numNodesAccessedDuringFinds, RecoverableLinkedListTracking<int>::prof.numNodesAccessedDuringFinds);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numSearchBarrier1, RecoverableLinkedListTracking<int>::prof.numSearchBarrier1);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numSearchBarrier2, RecoverableLinkedListTracking<int>::prof.numSearchBarrier2);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFindBarrier, RecoverableLinkedListTracking<int>::prof.numFindBarrier);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numSearchBarrier1, RecoverableLinkedListTracking<int>::prof.numSearchBarrier1);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numSearchBarrier2, RecoverableLinkedListTracking<int>::prof.numSearchBarrier2);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFindBarrier, RecoverableLinkedListTracking<int>::prof.numFindBarrier);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFlush, RecoverableLinkedListTracking<int>::prof.numPwb);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFlushLow, RecoverableLinkedListTracking<int>::prof.numPwbLow);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFlushMedium, RecoverableLinkedListTracking<int>::prof.numPwbMedium);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFlushHigh, RecoverableLinkedListTracking<int>::prof.numPwbHigh);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numBarrier, RecoverableLinkedListTracking<int>::prof.numBarrier);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFence, RecoverableLinkedListTracking<int>::prof.numPsync);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFlushHelp, RecoverableLinkedListTracking<int>::prof.numPwbHelp);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numBarrierHelp, RecoverableLinkedListTracking<int>::prof.numBarrierHelp);
    ADD(&totalNumRecoverableLinkedListTrackingActions_numFenceHelp, RecoverableLinkedListTracking<int>::prof.numPsyncHelp);
	#endif    

    return NULL;
}

inline static void* startRoutineRecoverableLinkedListCapsules(void* argsInput){

    int thread_id = *(int*)argsInput;
    threadPin(thread_id);

    long numMyOps=0;

	#if defined(PROFILING)
    unsigned long numMyInsertsSuccessful = 0, numMyInsertsUnSuccessful = 0;
    unsigned long numMyDeletesSuccessful = 0, numMyDeletesUnSuccessful = 0;
    unsigned long numMyFindsSuccessful = 0, numMyFindsUnSuccessful = 0;
	#endif      

    RecoverableLinkedListCapsules<int>& list = recoverableLinkedListCapsules;
    
    int key;
    unsigned int seed = time(NULL) + thread_id;
    long op;
    
    fastRandomSetSeed(seed);

    while (run == false) {          // busy-wait to start "simultaneously"
        MFENCE();
        pthread_yield();
    }

    while(!stop){
        numMyOps++;
        op = fastRandomRange(1, 100);
        key = fastRandomRange(1, KEY_RANGE);

        if (op <= 100*INSERTS_PERCENTAGE) {
		#if defined(PROFILING)
            RecoverableLinkedListCapsules<int>::prof.numInsertOps++;
            if (list.Insert(key, thread_id) == true) {
                numMyInsertsSuccessful++;
            }
            else {
                numMyInsertsUnSuccessful++;
            }
		#else
            list.Insert(key, thread_id);
		#endif             
        }
        else if (op <= 100*INSERTS_PERCENTAGE+100*DELETES_PERCENTAGE) {
		#if defined(PROFILING)
            RecoverableLinkedListCapsules<int>::prof.numDeleteOps++;
            if (list.Delete(key, thread_id) == true) {
                numMyDeletesSuccessful++;
            }
            else {
                numMyDeletesUnSuccessful++;
            }
		#else
            list.Delete(key, thread_id);
		#endif             

        }
        else {
		#if defined(PROFILING)
            RecoverableLinkedListCapsules<int>::prof.numFindOps++;
            if (list.Find(key, thread_id) == true) {
                numMyFindsSuccessful++;
            }
            else {
                numMyFindsUnSuccessful++;
            }
		#else
            list.Find(key, thread_id);
		#endif
        }
    }

    ADD(&totalNumRecoverableLinkedListCapsulesActions, numMyOps);

	#if defined(PROFILING)
    ADD(&totalNumRecoverableLinkedListCapsulesActions_InsertsSuccessful, numMyInsertsSuccessful);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_InsertsUnSuccessful, numMyInsertsUnSuccessful);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_DeletesSuccessful, numMyDeletesSuccessful);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_DeletesUnSuccessful, numMyDeletesUnSuccessful);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_FindsSuccessful, numMyFindsSuccessful);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_FindsUnSuccessful, numMyFindsUnSuccessful);

    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchOps, RecoverableLinkedListCapsules<int>::prof.numSearchOps);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringSearches, RecoverableLinkedListCapsules<int>::prof.numNodesAccessedDuringSearches);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numInsertOps, RecoverableLinkedListCapsules<int>::prof.numInsertOps);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numInsertAttempts, RecoverableLinkedListCapsules<int>::prof.numInsertAttempts);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringInserts, RecoverableLinkedListCapsules<int>::prof.numNodesAccessedDuringInserts);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numDeleteOps, RecoverableLinkedListCapsules<int>::prof.numDeleteOps);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numDeleteAttempts, RecoverableLinkedListCapsules<int>::prof.numDeleteAttempts);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringDeletes, RecoverableLinkedListCapsules<int>::prof.numNodesAccessedDuringDeletes); 
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFindOps, RecoverableLinkedListCapsules<int>::prof.numFindOps);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numNodesAccessedDuringFinds, RecoverableLinkedListCapsules<int>::prof.numNodesAccessedDuringFinds);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier1, RecoverableLinkedListCapsules<int>::prof.numSearchBarrier1);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier2, RecoverableLinkedListCapsules<int>::prof.numSearchBarrier2);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier3, RecoverableLinkedListCapsules<int>::prof.numSearchBarrier3);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier4, RecoverableLinkedListCapsules<int>::prof.numSearchBarrier4);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numSearchBarrier5, RecoverableLinkedListCapsules<int>::prof.numSearchBarrier5);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFlush, RecoverableLinkedListCapsules<int>::prof.numPwb);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFlushLow, RecoverableLinkedListCapsules<int>::prof.numPwbLow);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFlushMedium, RecoverableLinkedListCapsules<int>::prof.numPwbMedium);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFlushHigh, RecoverableLinkedListCapsules<int>::prof.numPwbHigh);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numBarrier, RecoverableLinkedListCapsules<int>::prof.numBarrier);
    ADD(&totalNumRecoverableLinkedListCapsulesActions_numFence, RecoverableLinkedListCapsules<int>::prof.numPsync);
	#endif    

    return NULL;
}

//==========================================================================

void countRecoverableLinkedListTracking(){

    recoverableLinkedListTracking.initialize();

    run = false;
    stop = false;

    int i;

    for (i = 0; i < numThreads; i++) {
        arguments[i * PADDING] = i;
        if(pthread_create(&threads[i], NULL, startRoutineRecoverableLinkedListTracking, (void*)&arguments[i * PADDING])){
            cout << "Error occurred when creating thread" << i << endl;
            exit(1);
        }
    }

    threadPin(i);

    run = true;
    MFENCE();
    sleep(timeForRecord);
    stop=true;
    MFENCE();

    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    cout << totalNumRecoverableLinkedListTrackingActions/timeForRecord << endl;
    file << totalNumRecoverableLinkedListTrackingActions/timeForRecord << endl;

    #if defined(PROFILING) && defined(MANUAL_FLUSH)
    cout << "Average Flushes: " << (float) totalNumRecoverableLinkedListTrackingActions_numFlush/totalNumRecoverableLinkedListTrackingActions << endl;
    cout << "Average Flushes Low: " << (float) totalNumRecoverableLinkedListTrackingActions_numFlushLow/totalNumRecoverableLinkedListTrackingActions << endl;
    cout << "Average Flushes Medium: " << (float) totalNumRecoverableLinkedListTrackingActions_numFlushMedium/totalNumRecoverableLinkedListTrackingActions << endl;
    cout << "Average Flushes High: " << (float) totalNumRecoverableLinkedListTrackingActions_numFlushHigh/totalNumRecoverableLinkedListTrackingActions << endl;
    cout << "Average Fences: " << (float) totalNumRecoverableLinkedListTrackingActions_numFence/totalNumRecoverableLinkedListTrackingActions << endl;

    file << "Test Tracking-Flushes - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListTrackingActions_numFlush/totalNumRecoverableLinkedListTrackingActions << endl;
    file << "Test Tracking-Flushes-Low - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListTrackingActions_numFlushLow/totalNumRecoverableLinkedListTrackingActions << endl;
    file << "Test Tracking-Flushes-Medium - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListTrackingActions_numFlushMedium/totalNumRecoverableLinkedListTrackingActions << endl;
    file << "Test Tracking-Flushes-High - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListTrackingActions_numFlushHigh/totalNumRecoverableLinkedListTrackingActions << endl;
    file << "Test Tracking-Fence - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListTrackingActions_numFence/totalNumRecoverableLinkedListTrackingActions << endl;
	#endif     
}

void countRecoverableLinkedListCapsules(){

    recoverableLinkedListCapsules.initialize(0);

    run = false;
    stop = false;

    int i;

    for (i = 0; i < numThreads; i++) {
        arguments[i * PADDING] = i;
        if(pthread_create(&threads[i], NULL, startRoutineRecoverableLinkedListCapsules, (void*)&arguments[i * PADDING])){
            cout << "Error occurred when creating thread" << i << endl;
            exit(1);
        }
    }

    threadPin(i);

    run = true;
    MFENCE();
    sleep(timeForRecord);
    stop=true;
    MFENCE();

    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    cout << totalNumRecoverableLinkedListCapsulesActions/timeForRecord << endl;
    file << totalNumRecoverableLinkedListCapsulesActions/timeForRecord << endl;
	#if defined(PROFILING) && defined(MANUAL_FLUSH)
    cout << "Average Flushes: " << (float) totalNumRecoverableLinkedListCapsulesActions_numFlush/totalNumRecoverableLinkedListCapsulesActions << endl;
    cout << "Average Flushes Low: " << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushLow/totalNumRecoverableLinkedListCapsulesActions << endl;
    cout << "Average Flushes Medium: " << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushMedium/totalNumRecoverableLinkedListCapsulesActions << endl;
    cout << "Average Flushes High: " << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushHigh/totalNumRecoverableLinkedListCapsulesActions << endl;
    cout << "Average Fences: " << (float) totalNumRecoverableLinkedListCapsulesActions_numFence/totalNumRecoverableLinkedListCapsulesActions << endl;

    file << "Test Capsules-Opt-Flushes - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListCapsulesActions_numFlush/totalNumRecoverableLinkedListCapsulesActions << endl;
    file << "Test Capsules-Opt-Flushes-Low - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushLow/totalNumRecoverableLinkedListCapsulesActions << endl;
    file << "Test Capsules-Opt-Flushes-Medium - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushMedium/totalNumRecoverableLinkedListCapsulesActions << endl;
    file << "Test Capsules-Opt-Flushes-High - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListCapsulesActions_numFlushHigh/totalNumRecoverableLinkedListCapsulesActions << endl;
    file << "Test Capsules-Opt-Fence - Threads num: " << numThreads << endl;
    file << (float) totalNumRecoverableLinkedListCapsulesActions_numFence/totalNumRecoverableLinkedListCapsulesActions << endl;
	#endif
}

//==========================================================================

int main(int argc, char* argv[]){

	file.open(
            string("results/linked_list_results[") + 
            to_string(INSERTS_PERCENTAGE).substr(0,4) + string(".") +  
            to_string(DELETES_PERCENTAGE).substr(0,4) + string(".") + 
            to_string(KEY_RANGE) + "].txt", 
            ofstream::app);

	char* linkedListType = argv[1];
	numThreads = atoi(argv[2]);
    timeForRecord = atoi(argv[3]); // how many seconds to run the experiment for

	// Tracking
   	if(!strcmp(linkedListType, "Tracking")){
        file << "Test Tracking - Threads num: " << numThreads << endl;
        cout << "Test Tracking - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-nopsync")){
        file << "Test Tracking-nopsync - Threads num: " << numThreads << endl;
        cout << "Test Tracking-nopsync - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-nopwbs")){
        file << "Test Tracking-nopwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-nopwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-nolowpwbs")){
        file << "Test Tracking-nolowpwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-nolowpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-nolownomedpwbs")){
        file << "Test Tracking-nolownomedpwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-nolownomedpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-lowpwbs")){
        file << "Test Tracking-lowpwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-lowpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-medpwbs")){
        file << "Test Tracking-medpwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-medpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    } else if(!strcmp(linkedListType, "Tracking-highpwbs")){
        file << "Test Tracking-highpwbs - Threads num: " << numThreads << endl;
        cout << "Test Tracking-highpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListTracking();
    }    

    // Capsules
    else if(!strcmp(linkedListType, "Capsules-Opt")){
        file << "Test Capsules-Opt - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules")){
        file << "Test Capsules - Threads num: " << numThreads << endl;
        cout << "Test Capsules - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-nopsync")){
        file << "Test Capsules-Opt-nopsync - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-nopsync - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-nopwbs")){
        file << "Test Capsules-Opt-nopwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-nopwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-nolowpwbs")){
        file << "Test Capsules-Opt-nolowpwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-nolowpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-nolownomedpwbs")){
        file << "Test Capsules-Opt-nolownomedpwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-nolownomedpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-lowpwbs")){
        file << "Test Capsules-Opt-lowpwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-lowpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-medpwbs")){
        file << "Test Capsules-Opt-medpwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-medpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    } else if(!strcmp(linkedListType, "Capsules-Opt-highpwbs")){
        file << "Test Capsules-Opt-highpwbs - Threads num: " << numThreads << endl;
        cout << "Test Capsules-Opt-highpwbs - Threads num: " << numThreads << endl;
        countRecoverableLinkedListCapsules();
    }   

    // default
    else {
        cerr << linkedListType << " is not a valid list type." << endl;
    }
    
    return 0;
}