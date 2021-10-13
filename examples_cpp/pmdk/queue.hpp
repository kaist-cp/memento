
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/mutex.hpp>
#include <iostream>

using namespace pmem::obj;

class queue {
   struct node {
       p<int> value; // TODO: generic
       persistent_ptr<node> next;
   };
private:
    pmem::obj::mutex pmutex;
    persistent_ptr<node> head;
    persistent_ptr<node> tail;
public:
    void push(pool_base &pop, uint64_t value)
    {
        transaction::run(pop, [&] {
            auto n = make_persistent<node>();
            n->value = value;
            n->next = nullptr;
            if (head == nullptr && tail == nullptr) {
				head = tail = n;
			} else {
				tail->next = n;
				tail = n;
			}
        }, pmutex);
    }
   std::optional<int> pop(pool_base &pop)
   {
        std::optional<int> value = std::nullopt;
        transaction::run(pop, [&] {
            if (head == nullptr) {
                return; // EMPTY
            }
            value = head->value;
            auto next = head->next;
            delete_persistent<node>(head);
            head = next;
			if (head == nullptr)
				tail = nullptr;
        }, pmutex);
        return value;
   }

   void show(void) const
   {
       for (auto n = head; n != nullptr; n = n->next)
           std::cout << n->value << " ";
       std::cout << std::endl;
   }
};
