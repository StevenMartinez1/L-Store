from template.table import Table, Record
from template.index import Index
from template.lock import Lock

t_id_counter = 1

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        global t_id_counter
        self.id = t_id_counter
        t_id_counter = t_id_counter + 1
        self.lockList = []
        #print(self.id)
        #self.lock = Lock()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))





    def lock_resources(self):
        for query, args in self.queries:
            # get RID of the key
            self.RID = query.__self__.table.keyToRID[args[0]]
            lock = query.__self__.table.RIDToLocks[self.RID]
            
            if(query.__name__ == "select"): # means it is select
                result = lock.exclusive_lock_acquire()
                if result == True:
                    self.lockList.append(lock)
                return result

            if(query.__name__ == "update"): # means it is update
                result = lock.exclusive_lock_acquire()
                if result == True:
                    self.lockList.append(lock)
                return result

            if(query.__name__ == "increment"): # means it is select
                result = lock.exclusive_lock_acquire()
                if result == True:
                    self.lockList.append(lock)
                return result

            #if(query.__name__ == "sum"): # means it is select
                # use shared lock if possible and return True, if not possible return False
                #return lock.exclusive_lock_acquire()

    
    """
    def unlock_resources(self):
        for query, args in self.queries:
            # get RID of the key
            self.RID = query.table.keyToRID[args[0]]

            if (query.__name__ == "select"):  # means it is select
            # delete from shared
                query.table.lock.shared_lock_disable(self.RID, self.id)
            #shared_lock_disable(self.RID)
            if (query.__name__ == "update"):  # means it is update
                query.table.lock.exclusive_lock_disable(self.RID)
    """

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        if(self.lock_resources() == False):
            return self.abort()
        for query, args in self.queries:
            # if(query.__name__ == "increment"):
            #     continue
            #print(query.__self__.table.name)
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        #self.unlock_resources()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        #print("Makes Abort")
        for lock in self.lockList:
            lock.exclusive_lock_release()
        return False

    def commit(self):
        # TODO: commit to database
        #print("Makes Commit")

        for lock in self.lockList:
            lock.exclusive_lock_release()

        return True

