import math
import thread

class BST(object):


    def __init__(self, values):

        values.sort()
        split = values.__len__()/2

        leftList = values[0:split]
        rightList = values[split+1:]

        self.value = values[split]
        if not leftList:
            self.left = None
        else:
            self.left = BST(leftList)
        if not rightList:
            self.right = None
        else:
            self.right = BST(rightList)
        self.n = values.__len__()
        #self.height = int(math.ceil(math.log(values.__len__() + 1, 2))) - 1


    def find_insert(self, item, insert = True):
        if insert:
            self.n += 1
        if item == self.value:
            return True

        if item < self.value:
            if self.left is None:
                if insert:
                    self.left = BST([item])
                return False
            else:
                return self.left.find_insert(item)

        if item > self.value:
            if self.right is None:
                if insert:
                    self.right = BST([item])
                return False
            else:
                return self.right.find_insert(item)

        print "Find insert problem, it should be unreachable"


    def rebalance_tree(self):

        return ;



class Locked_BST(object):


    def __init__(self, values):

        self.tree = BST(values)
        self.lock = thread.allocate_lock()

    def find_insert(self, item, insert= True):
        self.lock.acquire()
        b = self.tree.find_insert(item, insert= insert)
        self.lock.release()
        return b;

    def insert_list(self, items):
        self.lock.acquire()
        for item in items:
            self.tree.find_insert(item)
        self.lock.release()
        return ;

    def __len__(self):
        return self.tree.n
