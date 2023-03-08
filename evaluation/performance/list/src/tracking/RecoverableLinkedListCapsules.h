/// @file RecoverableLinkedListCapsules.h
/// @authors Eleftherios Kosmas (ekosmas (at) csd.uoc.gr) and Ohad Ben-Baruch (ohadben (at) post.bgu.ac.il)
///
/// For a more detailed description see the original publication:
/// Hagit Attiya, Ohad Ben-Baruch, Panagiota Fatourou, Danny Hendler, and Eleftherios Kosmas. 
/// "Detectable Recovery of Lock-Free Data Structures". ACM SIGPLAN Notices. 
/// Principles and Practice of Parallel Programming (PPoPP) 2022.
///
/// This algorithm has been implemented using the capsules transformation. More details:
/// Naama Ben-David, Guy E Blelloch, Michal Friedman, and Yuanhao Wei. 2019.
/// Delay-free concurrency on faulty persistent memory. In 31st ACM Symp on
/// Parallelism in Algorithms and Architectures (SPAA). 253–264.


#ifndef RECOVERABLE_LINKED_LIST_CAPSULES_H_
#define RECOVERABLE_LINKED_LIST_CAPSULES_H_

#include "Utilities.h"
#include "p_utils.h"

template <class T> class RecoverableLinkedListCapsules{

	public:

#if defined(PROFILING)
		class OpProfiler {
			public: 
				unsigned long numSearchOps;
				unsigned long numNodesAccessedTmp;
				unsigned long numNodesAccessedDuringSearches;
				unsigned long numInsertOps;
				unsigned long numInsertAttempts;
				unsigned long numNodesAccessedDuringInserts;
				unsigned long numDeleteOps;
				unsigned long numDeleteAttempts;
				unsigned long numNodesAccessedDuringDeletes;			
				unsigned long numFindOps;
				unsigned long numNodesAccessedDuringFinds;
				unsigned long numSearchBarrier1;
				unsigned long numSearchBarrier2;
				unsigned long numSearchBarrier3;
				unsigned long numSearchBarrier4;
				unsigned long numSearchBarrier5;
				unsigned long numPwb;
				unsigned long numPwbLow;
				unsigned long numPwbMedium;
				unsigned long numPwbHigh;
				unsigned long numBarrier;
				unsigned long numPsync;

				OpProfiler() {
					numSearchOps = 0;
					numNodesAccessedTmp = 0;
					numNodesAccessedDuringSearches = 0;
					numInsertOps = 0;
					numInsertAttempts = 0;
					numNodesAccessedDuringInserts = 0;
					numDeleteOps = 0;
					numDeleteAttempts = 0;
					numNodesAccessedDuringDeletes = 0;
					numFindOps = 0;
					numNodesAccessedDuringFinds = 0;
					numSearchBarrier1 = 0;
					numSearchBarrier2 = 0;
					numSearchBarrier3 = 0;
					numSearchBarrier4 = 0;
					numSearchBarrier5 = 0;
					numPwb = 0;
					numPwbLow = 0;
					numPwbMedium = 0;
					numPwbHigh = 0;
					numBarrier = 0;
					numPsync = 0;
				}
		};

		static thread_local OpProfiler prof;	
#endif				

		//====================Start Node Class==========================//
		class alignas(CACHE_LINE_SIZE) Node {
			public:
				RCas<Node*> volatile next;
				T value;
				Node(T val) {
					value = val;
					rcas_init(next);
				}
				Node() {
					value = T();
					rcas_init(next);
				}
		};
		//====================End Node Class==========================//

		RecoverableLinkedListCapsules() {

			init_closures();
			init_rcas_ann();

			head = new Node(INT_MIN);
			tail = new Node(INT_MAX);
		    MANUAL(PWB(tail);)
		    MANUAL(PFENCE();)
            rcas_init(head->next, tail);
            MANUAL(PWB(head);)
            MANUAL(PFENCE();)
            MANUAL(PWB(&head);)
            MANUAL(PFENCE();)
            MANUAL(PWB(&tail);)
            MANUAL(PSYNC();)
		}

		//---------------------------------
		Node* Search(int search_key, Node** left_node, int32_t threadID) {					
			Node *left_node_next, *right_node, *prev_left_node;

			(*left_node) = NULL;
			left_node_next = NULL;															// in order just to avoid warning

														#if defined(PROFILING)
														prof.numNodesAccessedTmp = 0;
														#endif		
			do {
				Node* t = head;
				Node* t_next = rcas_read(t->next);											
														#if defined(PROFILING)
														prof.numSearchOps++;
														prof.numNodesAccessedTmp++;
														#endif				
				do {
					if (is_marked_reference(t_next) == false) {
						prev_left_node = (*left_node);
						(*left_node) = t;
						left_node_next = t_next;
					}
					else {
						MANUAL(PWB_HIGH(&(t->next)));										// flush marking before physical remove
						MANUAL(PFENCE();)
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numSearchBarrier5++;
														prof.numBarrier++;
														prof.numPwb++;
														prof.numPwbHigh++;
														prof.numPsync++;
														#endif	
					}
					t = get_unmarked_reference(t_next);
					if (t == tail) break;				
					t_next = rcas_read(t->next);											
														#if defined(PROFILING)
														prof.numNodesAccessedTmp++;
														#endif						
				} while(is_marked_reference(t_next) == true || t->value < search_key);
				right_node = t;

				if (left_node_next == right_node && is_marked_reference(rcas_read(right_node->next)) == false) {
					MANUAL(PWB_HIGH(&((*left_node)->next));)
					MANUAL(if (prev_left_node) {PWB_HIGH(&(prev_left_node->next));})
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbHigh++;
														if (prev_left_node) {
															prof.numPwb++;
															prof.numPwbHigh++;
														}
														#endif							
					return right_node;			
				}

				if (rcas_cas(&((*left_node)->next), left_node_next, right_node, threadID, get_capsule_number(threadID))){
					MANUAL(PWB_LOW(&((*left_node)->next))); 
					MANUAL(PFENCE());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}
														#endif						
					if(is_marked_reference(rcas_read(right_node->next)) == false) {
						MANUAL(PWB_LOW(&((*left_node)->next));)
						MANUAL(if (prev_left_node) {PWB_LOW(&(prev_left_node->next));})
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														if (prev_left_node) {
															prof.numPwb++;
															prof.numPwbLow++;
														}
														#endif					
						return right_node;
					}
				}
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														else if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}
														#endif
			} while(1);
		}


		//---------------------------------
		bool Find(int search_key, int32_t threadID) {
			Node* right_node, *left_node;

			right_node = Search(search_key, &left_node, threadID);
														#if defined(PROFILING)
														prof.numNodesAccessedDuringFinds += prof.numNodesAccessedTmp;
														#endif
			MANUAL(PDETECT());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPsync++;
														#endif
			WFLUSH(PDETECT());
			if ((right_node == tail) || right_node->value != search_key)				// since MIN_INT, MAX_INT is never used as a key, the first condition is not required
				return false;
			return true;
		}

		//---------------------------------
		bool Insert(int key, int32_t threadID){
			Node *new_node = new Node(key);	
			Node *right_node, *left_node;

			do {
				//CAS generator
				right_node = Search(key, &left_node, threadID);
														#if defined(PROFILING)
														prof.numInsertAttempts++;
														prof.numNodesAccessedDuringInserts += prof.numNodesAccessedTmp;
														#endif					
				if ((right_node != tail) && right_node->value == key) {					// since MIN_INT, MAX_INT is never used as a key, the first condition is not required
					capsule_boundary_opt(threadID);
					MANUAL(PDETECT());
					WFLUSH(PDETECT());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														#endif				
					return false;
				}

				rcas_init(new_node->next, right_node);								
				MANUAL(PWB_LOW(new_node));    											
				WFLUSH(PWB(new_node));    											
				capsule_boundary_opt(threadID, new_node, right_node);
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb+=2;
														prof.numPwbLow+=2;
														#endif		
				// CAS executer
				if (rcas_cas(&(left_node->next), right_node, new_node, threadID, get_capsule_number(threadID))) {
					MANUAL(PWB_MED(&(left_node->next)));
					MANUAL(PFENCE());								
					//wrap-up:
					capsule_boundary_opt(threadID);
					MANUAL(PDETECT());
					WFLUSH(PDETECT());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb+=2;
														prof.numPsync+=2;
														prof.numPwbMedium++;
														prof.numPwbLow++;
														if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}	
														#endif							
					return true;
				}
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														else if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}	
														#endif								
			} while(1);
		}

		//---------------------------------
		bool Delete(int search_key, int32_t threadID) {
			Node *right_node, *right_node_next, *left_node;

			do {
				//CAS generator
				right_node = Search(search_key, &left_node, threadID);
														#if defined(PROFILING)
														prof.numDeleteAttempts++;
														prof.numNodesAccessedDuringDeletes += prof.numNodesAccessedTmp;
														#endif
				if ((right_node == tail) || right_node->value != search_key) {			// since MIN_INT, MAX_INT is never used as a key, the first condition is not required
					capsule_boundary_opt(threadID);
					MANUAL(PDETECT());
					WFLUSH(PDETECT());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														#endif							
					return false;
				}
				right_node_next = rcas_read(right_node->next);
				if (is_marked_reference(right_node_next) == false){
					capsule_boundary_opt(threadID, right_node, right_node_next);		
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														#endif						
					// CAS executer
					if (rcas_cas(&(right_node->next), right_node_next, get_marked_reference(right_node_next))) {
						MANUAL(PWB_LOW(&(right_node->next)));								
						MANUAL(PFENCE());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}	
														#endif	
						break;
					}
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														else if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}	
														#endif						
				}
			} while(1);

			//Wrap-up
			if(!rcas_cas(&(left_node->next), right_node, right_node_next,threadID, get_capsule_number(threadID))) {
				right_node = Search(right_node->value, &left_node, threadID);		
														#if defined(PROFILING)
														prof.numNodesAccessedDuringDeletes += prof.numNodesAccessedTmp;
														#endif	
			}
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														if (rcasPerformedPeristencyInstructions == true) {
															rcasPerformedPeristencyInstructions = false;							
															prof.numPwb+=2;
															prof.numPwbLow++;
															prof.numPwbMedium++;
															prof.numPsync++;
															prof.numBarrier++;
														}	
														#endif		
			capsule_boundary_opt(threadID);
			MANUAL(PDETECT());
			WFLUSH(PDETECT());
														#if defined(PROFILING) && defined(MANUAL_FLUSH)
														prof.numPwb++;
														prof.numPwbLow++;
														prof.numPsync++;
														#endif		
			return true;

		}

		//---------------------------------
		void initialize(int32_t threadID){
			long int seed = time(NULL) + 120;
			fastRandomSetSeed(seed);

			for (int i = 0; i < KEY_RANGE/2; i++) {
		  		int key = fastRandomRange(1, KEY_RANGE);
		  		Insert(key, threadID);							
			}
		}

		//---------------------------------
		void count_list_elements() {
			Node *iterator = rcas_read(head->next);
			int numNodes = 0, underDeletionNodes = 0;
			while (iterator != tail) {
				numNodes++;
				if (is_marked_reference(rcas_read(iterator->next)) == true) {
					numNodes--;
					underDeletionNodes++;
				}

				iterator = get_unmarked_reference(rcas_read(iterator->next));
			}

			printf ("Capsules - nodes: %d, under_deletion: %d \n", numNodes, underDeletionNodes);
		}

	private:
		// CHANGED
		Node* volatile head CACHE_ALIGN;	char padding1[PAD_CACHE(sizeof(Node*))];
		Node* volatile tail CACHE_ALIGN;	char padding2[PAD_CACHE(sizeof(Node*))];

		// ---------------------------------
		inline bool is_marked_reference(Node* addr) {
			long arg_addr = (long) addr;
			long marked_addr = arg_addr | 1u;

			if(marked_addr == arg_addr){
				return true;
			}
			return false;
		}

		// ---------------------------------
		inline Node* get_unmarked_reference(Node* addr) {
			long arg_addr = (long)addr;
			long marked_addr = arg_addr | 1u;

			if(marked_addr == arg_addr){
				arg_addr ^= (1u << 0);
			}

			return (Node*)arg_addr;
		}

		// ---------------------------------
		inline Node* get_marked_reference(Node* addr) {
			long arg_addr = (long)addr;

			arg_addr |= (1u << 0);

			return (Node*)arg_addr;
		}
};

#if defined(PROFILING)
RecoverableLinkedListCapsules<int>::OpProfiler tmpProfilerRecoverableLinkedListCapsules;
template<> thread_local RecoverableLinkedListCapsules<int>::OpProfiler RecoverableLinkedListCapsules<int>::prof CACHE_ALIGN = tmpProfilerRecoverableLinkedListCapsules;
#endif

#endif /* RECOVERABLE_LINKED_LIST_CAPSULES_H_ */
