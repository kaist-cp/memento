/// @file RecoverableLinkedListTracking.h
/// @authors Eleftherios Kosmas (ekosmas (at) csd.uoc.gr) and Ohad Ben-Baruch (ohadben (at) post.bgu.ac.il)
///
/// For a more detailed description see the original publication:
/// Hagit Attiya, Ohad Ben-Baruch, Panagiota Fatourou, Danny Hendler, and Eleftherios Kosmas. 
/// "Detectable Recovery of Lock-Free Data Structures". ACM SIGPLAN Notices. 
/// Principles and Practice of Parallel Programming (PPoPP) 2022.

#ifndef RECOVERABLE_LINKED_LIST_TRACKING_H_
#define RECOVERABLE_LINKED_LIST_TRACKING_H_

#include "Utilities.h"

template <class T> class RecoverableLinkedListTracking{

	private:
		enum {FALSE_RESULT, TRUE_RESULT, BOT_RESULT=-1};
		enum OpType {INSERT_OP_TYPE, DELETE_OP_TYPE, FIND_OP_TYPE};

	public:
		class Info;

#if defined(PROFILING)
		class OpProfiler {
			public: 
				unsigned long numNodesAccessedTmp;
				unsigned long numNodesAccessedDuringSearches;
				unsigned long numInsertOps;
				unsigned long numInsertAttempts;
				unsigned long numNodesAccessedDuringInserts;
				unsigned long numDeleteOps;
				unsigned long numDeleteAttempts;
				unsigned long numNodesAccessedDuringDeletes;			
				unsigned long numFindOps;
				unsigned long numFindAttempts;
				unsigned long numNodesAccessedDuringFinds;
				unsigned long numSearchBarrier1;
				unsigned long numSearchBarrier2;
				unsigned long numFindBarrier;
				unsigned long numPwb;
				unsigned long numPwbLow;
				unsigned long numPwbMedium;
				unsigned long numPwbHigh;
				unsigned long numBarrier;
				unsigned long numPsync;	
				unsigned long numPwbHelp;
				unsigned long numBarrierHelp;
				unsigned long numPsyncHelp;			

				OpProfiler() {
					numNodesAccessedTmp = 0;
					numNodesAccessedDuringSearches = 0;
					numInsertOps = 0;
					numInsertAttempts = 0;
					numNodesAccessedDuringInserts = 0;
					numDeleteOps = 0;
					numDeleteAttempts = 0;
					numNodesAccessedDuringDeletes = 0;
					numFindOps = 0;
					numFindAttempts = 0;
					numNodesAccessedDuringFinds = 0;
					numSearchBarrier1 = 0;
					numSearchBarrier2 = 0;
					numFindBarrier = 0;
					numPwb = 0;
					numPwbLow = 0;
					numPwbMedium = 0;
					numPwbHigh = 0;
					numBarrier = 0;
					numPsync = 0;
					numPwbHelp = 0;
					numBarrierHelp = 0;
					numPsyncHelp = 0;
				}
		};

		static thread_local OpProfiler prof;	
#endif	

		//====================Start Node Class==========================//
		class Node {
			public:
				Node* volatile next;
				T value;
				Info* volatile info;

				Node(T val, Node *nx) {
					value = val;
					next = nx;
					info = NULL;
				}
				Node(T val) {
					value = val;
					next = NULL;
					info = NULL;
				}
				Node() {
					value = T();
					next = NULL;
					info = NULL;
				}
		};
		//====================End Node Class==========================//
		//====================Start Info Class==========================//
		class Info {
			public:
				Node* pred; 
				Node* curr;
				Node* new_nd;
				Info* pred_info;
				Info* curr_info;
				T result;
				OpType op_type;

				Info() {
					pred = NULL;
					curr = NULL;
					new_nd = NULL;
					curr_info = NULL;
					result = BOT_RESULT;
				}
		};
		//====================End Info Class==========================//
		//====================Start TypeCP Class==========================//
		class alignas(CACHE_LINE_SIZE) TypeCP {
			public:
				int CP; 
				//char padding[PAD_CACHE(sizeof(int))];

				TypeCP() {
					CP = 0;
				}
		};
		//====================End TypeCP Class==========================//
		//====================Start TypeRD Class==========================//
		class alignas(CACHE_LINE_SIZE) TypeRD {
			public:
				Info* RD; 
				//char padding[PAD_CACHE(sizeof(Info *))];

				TypeRD() {
					RD = NULL;
				}
		};
		//====================End TypeRD Class==========================//


		RecoverableLinkedListTracking() {
			CP = new TypeCP [MAX_THREADS];
			RD = new TypeRD [MAX_THREADS];

			Node* dummy1 = new Node(INT_MIN);
			Node* dummy2 = new Node(INT_MAX);
            
            dummy1->next = dummy2;					
            dummy1->info = new Info();				
            dummy1->info->new_nd = new Node();		
            MANUAL(PWB(dummy1->info->new_nd);)	
            MANUAL(PWB(dummy1->info);)			
		    MANUAL(PWB(dummy1);)
		    MANUAL(PFENCE();)		
		    
		    dummy2->info = new Info();				
            MANUAL(PWB(dummy2->info);)			
		    MANUAL(PWB(dummy2);)				
		    MANUAL(PFENCE();)
            
            head = dummy1;
            MANUAL(PWB(&head);)
            MANUAL(PSYNC();)				
		}

		//---------------------------------
		Node* Search(T search_value, Node** pred, Info** pred_info, Info** curr_info) {
			Node* curr;
			Node* predpred;
    		*pred = NULL;
		    curr = head;
		    *curr_info = curr->info;
														#if defined(PROFILING)
														prof.numNodesAccessedTmp = 1;
														#endif		
		    while (curr->value < search_value) {
		    	predpred = *pred;								
		    	*pred = curr;									
		    	*pred_info = *curr_info;						
			   	curr = curr->next;								
		    	*curr_info = curr->info;
														#if defined(PROFILING)
														prof.numNodesAccessedTmp++;
														#endif		    	
		    }

		    return curr;
		}


		//---------------------------------
		bool Find(T search_value, int tid) {
			Node *curr, *pred;
			Info *curr_info, *pred_info;
			Info *op_info = new Info();
			op_info->op_type = FIND_OP_TYPE;

			RD[tid].RD = NULL;
			MANUAL(PWB_LOW(&RD[tid].RD);)
			MANUAL(PFENCE();)
			CP[tid].CP = 1;
			MANUAL(PWB_LOW(&CP[tid].CP);)		
			MANUAL(PSYNC();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif

			while(1) {
				// GATHER PHASE
				curr = Search(search_value, &pred, &pred_info, &curr_info);
														#if defined(PROFILING)
														prof.numFindAttempts++;
														prof.numNodesAccessedDuringFinds += prof.numNodesAccessedTmp;
														#endif				
				// HELPING PHASE
				if (is_marked_reference(curr_info) == true) {						// help the other operation
					HelpOp(get_unmarked_reference(curr_info), true);					
					continue;
				}

				op_info->result = (curr->value == search_value);								
				MANUAL(PWB_LOW(op_info);)
				MANUAL(PFENCE();)
				RD[tid].RD = op_info;
				MANUAL(PWB_LOW(&RD[tid].RD);)
				MANUAL(PDETECT();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif
				return (op_info->result);
			}
		}

		//---------------------------------

		bool Insert(T value, int tid) {
			Node *curr, *pred;
			Info *curr_info, *pred_info;
			Node *new_curr;
			Node *new_node;
			
			Info *op_info = new Info();
			op_info->op_type = INSERT_OP_TYPE;

			RD[tid].RD = NULL;															
			MANUAL(PWB_LOW(&RD[tid].RD);)
			MANUAL(PFENCE();)
			CP[tid].CP = 1;
			MANUAL(PWB_LOW(&CP[tid].CP);)
			MANUAL(PSYNC();)														
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif				

			while (1) {
				// GATHER PHASE
				curr = Search(value, &pred, &pred_info, &curr_info);					// search for right location to insert
														#if defined(PROFILING)
														prof.numInsertAttempts++;
														prof.numNodesAccessedDuringInserts += prof.numNodesAccessedTmp;
														#endif			
				// HELPING PHASE
				if (is_marked_reference(pred_info) == true) {							// help the other operation
					HelpOp(get_unmarked_reference(pred_info), true);													
				} 
				else if (is_marked_reference(curr_info) == true) {						// help the other operation
					HelpOp(get_unmarked_reference(curr_info), true);													
				}
				// UNSUCCESSFUL INSERT
				else if (curr->value == value) {										// value already in the list
					op_info->result = FALSE_RESULT;
					MANUAL(PWB_LOW(op_info);)														
					MANUAL(PFENCE();)
					RD[tid].RD = op_info;												
					MANUAL(PWB_LOW(&RD[tid].RD);)
					MANUAL(PDETECT();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif			
					return false;
				}
				else {																	
					// NewSet
					new_curr = new Node();												// make a copy for the successor of the new node
					new_curr->value = curr->value;										// update the copy of curr
					new_curr->next = curr->next;
					new_curr->info = get_marked_reference(op_info);
					MANUAL(PWB_LOW(new_curr);)
					new_node = new Node(value, new_curr);								// new node to insert
					new_node->info = get_marked_reference(op_info);						
					MANUAL(PWB_LOW(new_node);)

					// OpInfo
					op_info->pred = pred;
					op_info->curr = curr;
					op_info->new_nd = new_node;
					op_info->result = BOT_RESULT;
					op_info->pred_info = pred_info;
					op_info->curr_info = curr_info;
					MANUAL(PWB_LOW(op_info);)
					MANUAL(PFENCE();)
					RD[tid].RD = op_info;													
					MANUAL(PWB_LOW(&RD[tid].RD);)
					MANUAL(PSYNC();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb+=4;
														prof.numPwbLow+=4;
														prof.numBarrier++;
														prof.numPsync+=2;
														#endif
					HelpOp(op_info, false);
					if (op_info->result != BOT_RESULT) {								// SUCCESSFUL INSERT
						return op_info->result;
					}

					op_info = new Info();
					op_info->op_type = INSERT_OP_TYPE;				
				}
			}
		}

		//---------------------------------
		bool Delete(T value, int tid) {
			Node *curr, *pred;
			Info *curr_info, *pred_info;
			
			Info *op_info = new Info();
			op_info->op_type = DELETE_OP_TYPE;

			
			RD[tid].RD = NULL;																			
			MANUAL(PWB_LOW(&RD[tid].RD);)
			MANUAL(PFENCE();)
			CP[tid].CP = 1;
			MANUAL(PWB_LOW(&CP[tid].CP);)
			MANUAL(PSYNC();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif			

			while (1) {
				// GATHER PHASE
				curr = Search(value, &pred, &pred_info, &curr_info);					// search for right location to insert
														#if defined(PROFILING)
														prof.numDeleteAttempts++;
														prof.numNodesAccessedDuringDeletes += prof.numNodesAccessedTmp;
														#endif	
				// HELPING PHASE
				if (is_marked_reference(pred_info) == true) {							// help the other operation
					HelpOp(get_unmarked_reference(pred_info), true);													
				} 
				else if (is_marked_reference(curr_info) == true) {						// help the other operation
					HelpOp(get_unmarked_reference(curr_info), true);													
				}
				// UNSUCCESSFUL DELETE
				else if (curr->value != value) {										// value already in the list
					op_info -> result = FALSE_RESULT;
					MANUAL(PWB_LOW(op_info);)																						
					MANUAL(PFENCE();)
					RD[tid].RD = op_info;													
					MANUAL(PWB_LOW(&RD[tid].RD);)	
					MANUAL(PDETECT();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif			
					return false;
				}
				else {
					op_info->pred = pred;
					op_info->curr = curr;
					op_info->pred_info = pred_info;
					op_info->curr_info = curr_info;
					op_info->result = BOT_RESULT;
					MANUAL(PWB_LOW(op_info);)												
					MANUAL(PFENCE();)
					RD[tid].RD = op_info;													
					MANUAL(PWB_LOW(&RD[tid].RD);)
					MANUAL(PSYNC();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numBarrier++;
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														prof.numPsync+=2;
														#endif					
					HelpOp(op_info, false);
					if (op_info->result != BOT_RESULT) {								// SUCCESSFUL DELETE
						return op_info->result;
					}

					op_info = new Info();
					op_info->op_type = DELETE_OP_TYPE;	
				}
			}
		}

		//---------------------------------
		void initialize(){
			long int seed = time(NULL) + 120;
			fastRandomSetSeed(seed);

			for (int i = 0; i < KEY_RANGE/2; i++) {
		  		int value = fastRandomRange(1, KEY_RANGE);  		
		  		Insert(value, 0);
			}
		}

		//---------------------------------
		void count_list_elements() {
			Node * iterator = head->next;
			int numNodes = 0, tagedNodes = 0;

			cout << "Counting of elements started!" << endl; fflush(stdout);

			while (iterator->value != INT_MAX) {
				numNodes++;
				if (is_marked_reference(iterator->info) == true) {
					tagedNodes++;
				}

				iterator = iterator->next;				
			}

			printf ("Tracking - nodes: %d, tagged nodes: %d \n", numNodes, tagedNodes);
		}

	private:
		Node* volatile head CACHE_ALIGN;	char padding_head[PAD_CACHE(sizeof(Node*))];	
		TypeCP *CP CACHE_ALIGN;				char padding_CP[PAD_CACHE(sizeof(TypeCP*))];
		TypeRD *RD CACHE_ALIGN;				char padding_RD[PAD_CACHE(sizeof(TypeRD*))];


		// ---------------------------------
		inline bool HelpOp (Info *op_info, bool helper) {
			// TAGGING PHASE
			// try to tag pred		
			bool res = CAS(&(op_info->pred->info), op_info->pred_info, get_marked_reference(op_info));					// mark CAS
			MANUAL(PWB_MED(&(op_info->pred->info));)												
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbMedium++;
														if (helper) prof.numPwbHelp++;
														#endif

			Info* info = op_info->pred->info;
			if ( res || info == get_marked_reference(op_info) ) {							// op_info->pred is successfully marked
				// try to mark curr for removal. In case of Insert operation, it is replaced with a copy
				bool res = CAS(&(op_info->curr->info), op_info->curr_info, get_marked_reference(op_info));			// mark CAS
				MANUAL(PWB_MED(&(op_info->curr->info));)																	
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbMedium++;
														if (helper) prof.numPwbHelp++;
														#endif				

				Info* info = op_info->curr->info;
				if ( res || info == get_marked_reference(op_info) ) {							// op_info->curr is successfully marked
					Node *other;																// set other to the node pred needs to point to, according to the operation type

					if (op_info->new_nd != NULL) {
						other = op_info->new_nd;
					} else {
						other = op_info->curr->next;
					}

					MANUAL(PSYNC();)
					// UPDATE PHASE
					CAS(&(op_info->pred->next), op_info->curr, other);							// swing pred->next to other using CAS
					MANUAL(PWB_MED(&(op_info->pred->next));)										
					op_info->result = TRUE_RESULT;												// announce that operation completed
					MANUAL(PWB_LOW(&(op_info->result));)												
					MANUAL(PSYNC();)

					// CLEANUP PHASE
					CAS(&(op_info->pred->info), get_marked_reference(op_info), op_info);		// untag CAS of pred

														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPsync+=2;
														prof.numPwb+=2;
														prof.numPwbLow++;
														prof.numPwbMedium++;
														if (helper) { prof.numPsyncHelp+=2; prof.numPwbHelp+=2; }
														#endif				


					if (op_info->op_type == INSERT_OP_TYPE) {
						CAS(&(op_info->new_nd->info), get_marked_reference(op_info), op_info);				// untag CAS of new_nd
						CAS(&(op_info->new_nd->next->info), get_marked_reference(op_info), op_info);
					}
					
					return true;
				}
				// BACKTRACK PHASE
				else {																			// the mark CAS failed
					CAS(&(op_info->pred->info), get_marked_reference(op_info), op_info);		// backtrack CAS
					MANUAL(PWB_LOW(&(op_info->pred->info));)
					MANUAL(PSYNC();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														if (helper) { prof.numPwbHelp++; prof.numPsyncHelp++;}
														#endif						
				}
			}

			return false;																		// tell INSERT/DELETE routine to try again
		}


		// ---------------------------------
		inline bool is_marked_reference(Info* addr) {
			long arg_addr = (long) addr;
			long marked_addr = arg_addr | 1u;

			if(marked_addr == arg_addr){
				return true;
			}
			return false;
		}

		// ---------------------------------
		inline Info* get_unmarked_reference(Info* addr) {
			long arg_addr = (long)addr;
			long marked_addr = arg_addr | 1u;

			if(marked_addr == arg_addr){
				arg_addr ^= (1u << 0);
			}

			return (Info*)arg_addr;
		}

		// ---------------------------------
		inline Info* get_marked_reference(Info* addr) {
			long arg_addr = (long)addr;

			arg_addr |= (1u << 0);

			return (Info*)arg_addr;
		}
};

#if defined(PROFILING)
RecoverableLinkedListTracking<int>::OpProfiler tmpProfilerRecoverableLinkedListTracking;
template<> thread_local RecoverableLinkedListTracking<int>::OpProfiler RecoverableLinkedListTracking<int>::prof CACHE_ALIGN = tmpProfilerRecoverableLinkedListTracking;
#endif

#endif /* RECOVERABLE_LINKED_LIST_TRACKING_H_ */
