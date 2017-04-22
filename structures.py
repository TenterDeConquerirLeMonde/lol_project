import math
import thread

class BST(object):


    def __init__(self, values, parentNode= None):

        values.sort()
        split = values.__len__()/2

        leftList = []
        rightList = []

        for i in range(0, values.__len__()):
            if i < split :
                leftList.append(values[i])
            if i > split :
                rightList.append(values[i])

        self.value = values[split]
        if not leftList:
            self.left = None
        else:
            self.left = BST(leftList, parentNode= self)
        if not rightList:
            self.right = None
        else:
            self.right = BST(rightList, parentNode= self)
        self.parent = parentNode
        self.n = values.__len__()
        self.height = int(math.ceil(math.log(values.__len__() + 1, 2))) - 1


    def find_insert(self, item, insert = True):
        if item == self.value:
            return True

        if item < self.value:
            if self.left is None:
                if insert:
                    self.left = BST([item], parentNode=self)
                return False
            else:
                return self.left.find_insert(item)

        if item > self.value:
            if self.right is None:
                if insert:
                    self.right = BST([item], parentNode=self)
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