/* =============================================================================
 *
 * client.c
 *
 * =============================================================================
 *
 * Copyright (C) Stanford University, 2006.  All Rights Reserved.
 * Author: Chi Cao Minh
 *
 * =============================================================================
 *
 * For the license of bayes/sort.h and bayes/sort.c, please see the header
 * of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of kmeans, please see kmeans/LICENSE.kmeans
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of ssca2, please see ssca2/COPYRIGHT
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/mt19937ar.c and lib/mt19937ar.h, please see the
 * header of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/rbtree.h and lib/rbtree.c, please see
 * lib/LEGALNOTICE.rbtree and lib/LICENSE.rbtree
 * 
 * ------------------------------------------------------------------------
 * 
 * Unless otherwise noted, the following license applies to STAMP files:
 * 
 * Copyright (c) 2007, Stanford University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 * 
 *     * Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * =============================================================================
 */


#include <assert.h>
#include "action.h"
#include "client.h"
#include "manager.h"
#include "reservation.h"
#include "../lib/thread.h"
#include "../lib/types.h"


/* =============================================================================
 * client_alloc
 * -- Returns NULL on failure
 * =============================================================================
 */
client_t*
client_alloc (long id,
              manager_t* managerPtr,
              long numOperation,
              long numQueryPerTransaction,
              long queryRange,
              long percentUser)
{
    client_t* clientPtr;

    clientPtr = (client_t*)malloc(sizeof(client_t));
    if (clientPtr == NULL) {
        return NULL;
    }

    clientPtr->randomPtr = random_alloc();
    if (clientPtr->randomPtr == NULL) {
        return NULL;
    }

    clientPtr->id = id;
    clientPtr->managerPtr = managerPtr;
    random_seed(clientPtr->randomPtr, id);
    clientPtr->numOperation = numOperation;
    clientPtr->numQueryPerTransaction = numQueryPerTransaction;
    clientPtr->queryRange = queryRange;
    clientPtr->percentUser = percentUser;

    return clientPtr;
}


/* =============================================================================
 * client_free
 * =============================================================================
 */
void
client_free (client_t* clientPtr)
{
    free(clientPtr);
}


/* =============================================================================
 * selectAction
 * =============================================================================
 */
static action_t
selectAction (long r, long percentUser)
{
    action_t action;

    if (r < percentUser) {
        action = ACTION_MAKE_RESERVATION;
    } else if (r & 1) {
        action = ACTION_DELETE_CUSTOMER;
    } else {
        action = ACTION_UPDATE_TABLES;
    }

    return action;
}


void make_reservation(manager_t* managerPtr, random_t*  randomPtr, long numQueryPerTransaction, long queryRange,
		long* types, long* ids){
		
		add_func_index(0);
		nvm_ptr_record(managerPtr, sizeof(manager_t*));
		nvm_ptr_record(randomPtr, sizeof(random_t*));
		ptr_para_record(&numQueryPerTransaction, sizeof(long));
		ptr_para_record(&queryRange, sizeof(long));

		long maxPrices[NUM_RESERVATION_TYPE] = { -1, -1, -1 };
                long maxIds[NUM_RESERVATION_TYPE] = { -1, -1, -1 };
                long n;
                long numQuery = random_generate(randomPtr) % numQueryPerTransaction + 1;
                long customerId = random_generate(randomPtr) % queryRange + 1;

		ptr_para_record(&numQuery, sizeof(long));
		ptr_para_record(&customerId, sizeof(long));

		for (n = 0; n < numQuery; n++) {
                    types[n] = random_generate(randomPtr) % NUM_RESERVATION_TYPE;
                    ids[n] = (random_generate(randomPtr) % queryRange) + 1;
                }

		ptr_para_record(&numQuery, sizeof(long));
                ptr_para_record(types,numQuery * sizeof(long));
                ptr_para_record(ids,numQuery * sizeof(long));

                bool_t isFound = FALSE;
                TM_BEGIN();
		t_lock();
                for (n = 0; n < numQuery; n++) {
                    long t = types[n];
                    long id = ids[n];
                    long price = -1;
                    switch (t) {
                        case RESERVATION_CAR:
                            if (MANAGER_QUERY_CAR(managerPtr, id) >= 0) {
                                price = MANAGER_QUERY_CAR_PRICE(managerPtr, id);
                            }
                            break;
                        case RESERVATION_FLIGHT:
                            if (MANAGER_QUERY_FLIGHT(managerPtr, id) >= 0) {
                                price = MANAGER_QUERY_FLIGHT_PRICE(managerPtr, id);
                            }
                            break;
                        case RESERVATION_ROOM:
                            if (MANAGER_QUERY_ROOM(managerPtr, id) >= 0) {
                                price = MANAGER_QUERY_ROOM_PRICE(managerPtr, id);
                            }
                            break;
                        default:
                            assert(0);
                    }
                    if (price > maxPrices[t]) {
                        maxPrices[t] = price;
                        maxIds[t] = id;
                        isFound = TRUE;
                    }
                } /* for n */
                if (isFound) {
                    MANAGER_ADD_CUSTOMER(managerPtr, customerId);
                }
                if (maxIds[RESERVATION_CAR] > 0) {
                    MANAGER_RESERVE_CAR(managerPtr,
                                        customerId, maxIds[RESERVATION_CAR]);
                }
                if (maxIds[RESERVATION_FLIGHT] > 0) {
                    MANAGER_RESERVE_FLIGHT(managerPtr,
                                           customerId, maxIds[RESERVATION_FLIGHT]);
                }
                if (maxIds[RESERVATION_ROOM] > 0) {
                    MANAGER_RESERVE_ROOM(managerPtr,
                                         customerId, maxIds[RESERVATION_ROOM]);
                }
		t_unlock();
                TM_END();
}


void delete_customer(manager_t* managerPtr, random_t*  randomPtr, long queryRange){
		long customerId = random_generate(randomPtr) % queryRange + 1;
                add_func_index(1);
                nvm_ptr_record(managerPtr, sizeof(manager_t*));
                nvm_ptr_record(randomPtr, sizeof(random_t*));

		ptr_para_record(&queryRange, sizeof(long));
                ptr_para_record(&customerId, sizeof(long));
		TM_BEGIN();
		t_lock();
                long bill = MANAGER_QUERY_CUSTOMER_BILL(managerPtr, customerId);
                if (bill >= 0) {
                    MANAGER_DELETE_CUSTOMER(managerPtr, customerId);
                }
		t_unlock();
                TM_END();
}


void update_table(manager_t* managerPtr, random_t*  randomPtr, long numQueryPerTransaction, long queryRange,
                long* types, long* ids, long* ops, long* prices){
                long numUpdate = random_generate(randomPtr) % numQueryPerTransaction + 1;
                long n;
                for (n = 0; n < numUpdate; n++) {
                    types[n] = random_generate(randomPtr) % NUM_RESERVATION_TYPE;
                    ids[n] = (random_generate(randomPtr) % queryRange) + 1;
                    ops[n] = random_generate(randomPtr) % 2;
                    if (ops[n]) {
                        prices[n] = ((random_generate(randomPtr) % 5) * 10) + 50;
                    }
                }

		add_func_index(2);
                nvm_ptr_record(managerPtr, sizeof(manager_t*));
                nvm_ptr_record(randomPtr, sizeof(random_t*));

                ptr_para_record(&numQueryPerTransaction, sizeof(long));
                ptr_para_record(&queryRange, sizeof(long));
		
		ptr_para_record(&numUpdate, sizeof(long));
                ptr_para_record(types,numUpdate * sizeof(long));
                ptr_para_record(ids, numUpdate * sizeof(long));
                ptr_para_record(ops,numUpdate * sizeof(long));
                ptr_para_record(prices, numUpdate * sizeof(long));




		TM_BEGIN();
		t_lock();
                for (n = 0; n < numUpdate; n++) {
                    long t = types[n];
                    long id = ids[n];
                    long doAdd = ops[n];
                    if (doAdd) {
                        long newPrice = prices[n];
                        switch (t) {
                            case RESERVATION_CAR:
                                MANAGER_ADD_CAR(managerPtr, id, 100, newPrice);
                                break;
                            case RESERVATION_FLIGHT:
                                MANAGER_ADD_FLIGHT(managerPtr, id, 100, newPrice);
                                break;
                            case RESERVATION_ROOM:
                                MANAGER_ADD_ROOM(managerPtr, id, 100, newPrice);
                                break;
                            default:
                                assert(0);
                        }
                    } else { /* do delete */
                        switch (t) {
                            case RESERVATION_CAR:
                                MANAGER_DELETE_CAR(managerPtr, id, 100);
                                break;
                            case RESERVATION_FLIGHT:
                                MANAGER_DELETE_FLIGHT(managerPtr, id);
                                break;
                            case RESERVATION_ROOM:
                                MANAGER_DELETE_ROOM(managerPtr, id, 100);
                                break;
                            default:
                                assert(0);
                        }
                    }
                }
		t_unlock();
                TM_END();
}


/* =============================================================================
 * client_run
 * -- Execute list operations on the database
 * =============================================================================
 */
void
client_run (void* argPtr)
{
    //TM_THREAD_ENTER();

    long myId = thread_getId();
    client_t* clientPtr = ((client_t**)argPtr)[myId];

    manager_t* managerPtr = clientPtr->managerPtr;
    random_t*  randomPtr  = clientPtr->randomPtr;

    long numOperation           = clientPtr->numOperation;
    long numQueryPerTransaction = clientPtr->numQueryPerTransaction;
    long queryRange             = clientPtr->queryRange;
    long percentUser            = clientPtr->percentUser;

    long* types  = (long*)P_MALLOC(numQueryPerTransaction * sizeof(long));
    long* ids    = (long*)P_MALLOC(numQueryPerTransaction * sizeof(long));
    long* ops    = (long*)P_MALLOC(numQueryPerTransaction * sizeof(long));
    long* prices = (long*)P_MALLOC(numQueryPerTransaction * sizeof(long));

    long i;

    for (i = 0; i < numOperation; i++) {

        long r = random_generate(randomPtr) % 100;
        action_t action = selectAction(r, percentUser);

        switch (action) {

            case ACTION_MAKE_RESERVATION: {
		make_reservation(managerPtr, randomPtr, numQueryPerTransaction, queryRange, types, ids);
                break;
            }

            case ACTION_DELETE_CUSTOMER: {
		delete_customer(managerPtr, randomPtr, queryRange);
                break;
            }

            case ACTION_UPDATE_TABLES: {
		update_table(managerPtr, randomPtr, numQueryPerTransaction, queryRange, types, ids, ops, prices);
                break;
            }

            default:
                assert(0);

        } /* switch (action) */

    } /* for i */

    //TM_THREAD_EXIT();
}


/* =============================================================================
 *
 * End of client.c
 *
 * =============================================================================
 */




