from random import choice, randint, sample, seed
#basic tree

class Node():
    
    def __init__(self, max_node_size):
        self.max_node_size = max_node_size
        self.is_leaf = True
        self.left = None
        self.right = None
        self.keys = []
        self.rids = []
        
    @property
    def is_leaf(self):
        return self.__is_leaf

    @is_leaf.setter
    def is_leaf(self, is_Leaf):
        if not is_Leaf:
            self.left = None
            self.right = None
        self.__is_leaf = is_Leaf

    @property
    def left(self):
        return self.__left

    @left.setter
    def left(self, left):
        self.__left = left

    @property
    def right(self):
        return self.__right

    @right.setter
    def right(self, right):
        self.__right = right

    def add(self, key, rid):
        if not self.keys: #if keys is empty (root)
            self.keys.append(key)
            self.rids.append([rid])
            return None
        
        if self.is_leaf:
            if type(rid) != list:
                rid = [rid]
            
        for i, item in enumerate(self.keys): #check for existing key in node
            if key == item:
                self.rids[i] += rid #if node exists add rid to rids[]
                break
            elif key < item: #if key is beginning of node and key doesnt already exist
                self.keys = self.keys[:i] + [key] + self.keys[i:] 
                self.rids = self.rids[:i] + [rid] + self.rids[i:]
                break
            elif i + 1 == len(self.keys): # check for end of node
                self.keys.append(key) #add key to end of node
                self.rids.append(rid)
                break
        
    
    def remove(self, key, rid):

        for i, item in enumerate(self.keys):
            if key == item:
                for j, itemRID in enumerate(self.rids[i]):
                    if rid == itemRID:
                        del(self.rids[i][j]) #perform the delete

                        if len(self.rids[i]) == 0: #if key is empty, remove key
                            del(self.keys[i])
                            self.rids.remove([]) 
                        
                        break
                break
        pass      

    def update_keys(self):
        if self.is_leaf:
            print("HMMMMM")
            return None
        for i in range(1,len(self.rids)):
            if i > len(self.keys):
                self.keys.append(self.rids[i].keys[0])
            else:
                if self.keys[i-1] != self.rids[i].keys[0]:
                    self.keys[i-1] = self.rids[i].keys[0]

    def split(self):
    #split and send to child nodes
        #left and right inherit number of objects
        left = Node(self.max_node_size) 
        right = Node(self.max_node_size)
        mid = (self.max_node_size + 1)//2

        left.keys = self.keys[:mid]
        right.keys = self.keys[mid:]

        if self.is_leaf == False:
            left.is_leaf = False
            right.is_leaf = False
            if self.max_node_size % 2 == 0:
                left.rids = self.rids[:mid +1]
                right.rids = self.rids[mid + 1:]
                self.keys = [right.keys.pop(0)]
            else:
                left.rids = self.rids[:mid]
                right.rids = self.rids[mid:]
                self.keys = [left.keys[-1]]
                
        else:
            left.rids = self.rids[:mid]
            right.rids = self.rids[mid:]
            self.keys = [right.keys[0]]

            left.right = right
            left.left = self.left

            right.left = left
            right.right = self.right

            if self.left != None:
                self.left.right = left

            if self.right != None:
                self.right.left = right
            


        self.rids = [left, right]
        self.is_leaf = False

    def is_full(self):
        return len(self.keys) == self.max_node_size + 1

    def key_helper(self, bool, ret=""):
        if bool:
            for val in bool:
                if val:
                    ret += "|  "
                else:
                    ret += "   "
            
            if bool[-1]:
                ret += "|--" 
            else:
                ret += "`--"
        
        ret += str(self.keys) + "\n"

        if not self.is_leaf:

            for val in self.rids:

                if val != self.rids[-1]:
                    bool.append(True)

                else:
                    bool.append(False) 

                if isinstance(val,Node):
                    ret = val.key_helper(bool, ret) 
                    bool.pop()

        return ret


    def get_keys(self):
        bool = []
        to_print = self.key_helper(bool)
        return to_print
            

class BPlusTree(object):

    def __init__(self, max_node_size = 4):
        self.root = Node(max_node_size)
        self.left = self.root
        self.right = self.root

    def _find(self, node, key): #return index of key and its rids
        
        for i, item in enumerate(node.keys):
            if key < item:
                return node.rids[i], i
                
        if i < len(node.rids) - 1:
            return node.rids[i+1], i+1
        else:
            return None, i


    def find(self, node, key): #return index of key and its rids
        return self._find(node, key)

    def _merge(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
            return None

        parent.rids.pop(index)
        pivot = child.keys[0]
        
        for i, item in enumerate(parent.keys):

            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                left = parent.rids[:i]
                right = parent.rids[i:]

                for rid in child.rids: 
                    left.append(rid)
                parent.rids = left + right
                break
            
            elif i+1 == len(parent.keys):

                parent.keys.append(pivot)

                for rid in child.rids:
                    parent.rids.append(rid)
                break
        
        if parent.is_full():

            parent.split()
            if parents and not parents[-1][0].is_full():
                # print("At merge")
                self._merge(parents, parent)

         
        
    def insert(self, key, rid):

        child = self.root
        prev_node = self.root
        parents = []

        while not child == None and not child.is_leaf:

            child, index = self._find(child, key)
            parents.append((prev_node, index))
            prev_node = child

        if child == None:
            parent = parents[-1][0]
            child = Node(parent.max_node_size)
            child.add(key,rid)
            parent.rids.append(child)
        else:
            child.add(key, rid)

        if child.is_full():

            child.split()

            if self.root == child:
                self.left = child.rids[0]
                self.right = child.rids[1]
            elif self.right == child:
                self.right = child.rids[1]
            elif self.left == child:
                self.left = child.rids[0]

            if parents and not parents[-1][0].is_full():
                self._merge(parents, child)

    def remove(self, key, rid):

        parents = []
        prev_node = self.root
        child = self.root

        while not child == None and not child.is_leaf:
            child, index = self._find(child, key)
            parents.append((prev_node, index))
            prev_node = child

        if child != None:
            child.remove(key,rid)
            
            if(len(child.keys) < child.max_node_size/2):
                self._balance(parents, child)

        return None

    def find_by_rid(self, rid):

        current_node = self.left
        key = None

        while key == None and current_node != None:

            for i ,rids in enumerate(current_node.rids):
                for cur_rid in rids:
                    if cur_rid == rid:
                        key = current_node.keys[i]
            
            current_node = current_node.right
            
        return key
        

    def _balance(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
            # print("EHHHHHHH")
            return None

        sibling_index = index
        to_the_left = False

        if index == len(parent.keys):
            index += -1
            sibling_index += -1
            to_the_left = True
        else:
            if parent.max_node_size % 2 == 0:
                sibling_index += 1
            elif len(parent.keys) == len(parent.rids) and index == len(parent.keys) - 1:
                index += -1
                sibling_index += -1
                to_the_left = True
            else:
                sibling_index += 1
                
        
        sibling = parent.rids[sibling_index]

        is_Leafs = False
        if sibling.is_leaf and child.is_leaf:
            is_Leafs = True

        flags = (to_the_left,is_Leafs)
        indexes = (index, sibling_index)

        if len(sibling.keys) <= (sibling.max_node_size + 1)/2:
            self.consolidate(indexes, child, sibling, parent, flags)

            if parent != self.root and len(parent.keys) < parent.max_node_size/2 :
                self._balance(parents, parent)
            elif parent == self.root and not parent.keys:
                if not parent.keys:
                    self.root = parent.rids[0]

        else:
            self.share(indexes, child, sibling, parent, flags)
    
    def consolidate(self, indexes, child, sibling, parent, flags):
        index, sibling_index = indexes
        to_the_left, is_Leafs = flags

        while sibling.keys or sibling.rids:
            if is_Leafs:
                if len(sibling.rids[0]) == 1:
                    child.add(sibling.keys.pop(0), sibling.rids.pop(0)[0])
                else:
                    child.add(sibling.keys.pop(0), sibling.rids.pop(0))
            else:
                if sibling.keys:
                    child.add(sibling.keys.pop(0), sibling.rids.pop(0))
                else:
                    if to_the_left:
                        child.rids.insert(i,sibling.rids.pop(0))
                    else:
                        child.rids.append(sibling.rids.pop(0))

        if not child.is_leaf and child.rids[0].is_leaf:
            child.update_keys()
        
        if is_Leafs:
            if to_the_left:
                child.left = sibling.left
                if sibling == self.left:
                    self.left = child.left

            else:
                child.right = sibling.right
                if sibling == self.right:
                    self.right = child.right

        del parent.rids[sibling_index]

        if to_the_left:
            del parent.keys[sibling_index]
        else:
            del parent.keys[index]

        if parent.rids[0].is_leaf:
            parent.update_keys()

    def share(self, indexes, child, sibling, parent, flags):
        index, sibling_index = indexes
        to_the_left, is_Leafs = flags

        while len(sibling.keys) > child.max_node_size/2 and len(child.keys) < child.max_node_size/2:
            if to_the_left:
                current_key = sibling.keys.pop()
                
                if is_Leafs:
                    if len(sibling.rids[-1]) == 1:
                        child.add(current_key, sibling.rids.pop()[0])
                    else:
                        child.add(current_key, sibling.rids.pop())
                else:
                    child.add(current_key, sibling.rids.pop())
            else:
                current_key = sibling.keys.pop(0)
                if is_Leafs:
                    if len(sibling.rids[0]) == 1:
                        child.add(current_key, sibling.rids.pop(0)[0])
                    else:
                        child.add(current_key, sibling.rids.pop(0))
                else:
                    child.add(current_key, sibling.rids.pop(0))

        if parent.rids[0].is_leaf:
            parent.update_keys()
        else:
            if to_the_left:
                parent.keys[index] = child.keys[0]
            else:
                parent.keys[index] = child.keys[-1]

        if not child.is_leaf and child.rids[0].is_leaf:
            child.update_keys()

    def bulk_search(self, start, end):

        current_node = self.root
        while not current_node == None and not current_node.is_leaf:
            current_node, index = self._find(current_node, start)
        
        rids_to_return = []
        # sum = 0
        current_key = start
        
        while current_key <= end and current_node != None:

            for i, key in enumerate(current_node.keys):

                if key >= current_key and key <= end:
                    current_key = key

                    rids_to_return += current_node.rids[i]
            
            current_node = current_node.right
                    
        return rids_to_return

    def sum_range(self, start, end):
        current_node = self.root
        while not current_node == None and not current_node.is_leaf:
            current_node, index = self._find(current_node, start)
        
        sum = 0
        # sum = 0
        current_key = start
        
        while current_key <= end and current_node != None:

            for i, key in enumerate(current_node.keys):

                if key >= current_key and key <= end:
                    current_key = key

                    for rid in current_node.rids:
                        sum += current_key
            
            current_node = current_node.right
                    
        return sum
        
    def get_all_leaves(self):

        current_node = self.left
        return_data = [self.root.keys]

        while current_node != None:
            return_data.append((current_node.keys,current_node.rids))
            print(current_node.keys,"next")
            current_node = current_node.right

        return return_data

    def get_rid(self, key):
        child = self.root

        while not child == None and not child.is_leaf:
            child, index = self._find(child, key)

        if child != None:
            for i, item in enumerate(child.keys):
                if key == item:
                    return child.rids[i]

        return None
    

    def get_keys(self):
        return self.root.get_keys()


def demo_treeNum():
    seed(123245)
    max_keys = int(input("\n Input max keys:"))
    bplustree = BPlusTree(max_keys)
    print("Start demo")
    keys = []

    for i in range(16):
        key = randint(0, 9000)
        while key in keys:
            key = randint(0, 9000)

        bplustree.insert(key,i)
        print(key, "Inserted at", i)
        keys.append((key,i))

    print(bplustree.get_keys())

    # deleted_keys = sample(keys, 2)
    
    # for key in deleted_keys:
    #     print(key)
    #     bplustree.remove(key[0],key[1])
    
    # print(bplustree.get_keys())

    while True:

        command = input("Enter a command: ")

        if command == 'exit':
            break
            
        elif command == 'insert':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            while(1):
                try: 
                    insertvalue = int(insertvalue)
                    insertRID = int(insertRID)
                    break
                except ValueError:
                    insertvalue = input("Enter key as interger: ")
                    insertRID = input("Enter RID as interger: ")

            bplustree.insert(insertvalue, insertRID)
            print("inserted")

        elif command == 'remove':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            while(1):
                try: 
                    insertvalue = int(insertvalue)
                    insertRID = int(insertRID)
                    break
                except ValueError:
                    insertvalue = input("Enter key as interger: ")
                    insertRID = input("Enter RID as interger: ")
            bplustree.remove(insertvalue, insertRID)
            print(bplustree.get_rid(insertvalue))

        elif command == 'find':
            readRID = input("Enter key value to display RIDs: ")
            while(1):
                try: 
                    readRID = int(readRID)
                    break
                except ValueError:
                    readRID = input("Enter key value as integar to display RIDs: ")
            print(bplustree.get_rid(readRID))
        
        elif command == 'keys':
            print(bplustree.get_keys())
        
        else:
            print("Not valid input")


        print("\n___________________________________________\n")
    

def demo_tree():
    seed(12345)
    bplustree = BPlusTree(4)
    print("Start demo")
    keys = []

    for i in range(450):
        key = randint(0, 9000)

        bplustree.insert(key,i)
        # print(key, "Inserted at", i)
        keys.append((key,i))
    # print(keys)
    print(bplustree.get_keys())

    for key in keys:
        rid = bplustree.get_rid(key[0])

        if key[1] in rid:
            pass
            # print("Successful find")
        else:
            print("Error of key", key[0],"Rid is",key[1],"should be",rid)  


    deleted_keys = sample(keys, 100)
    for i, key in enumerate(deleted_keys):
        print(key)
        # if (i > 35 and i < 40):
        #     print(bplustree.get_keys())

        bplustree.remove(key[0],key[1])
        # if (i > 35 and i < 40):
        #     print(bplustree.get_keys())
    
    # print(bplustree.get_keys())

    for key in keys:
        rid = bplustree.get_rid(key[0])
        if rid == None:
            if key in deleted_keys:
                print("Key successful deleted")
            else: 
                print("Key should be here", key)
        elif key in deleted_keys:
            if key[1] in rid: 
                print("Key should be deleted", key)
                print(bplustree.get_keys())
            else:
                print("Key successful deleted one rid")
            # print("Successful find")
        else:
            if key[1] in rid:
                pass
            else:
                print("Error of key", key[0],"Rid is",rid,"should be",key[1])  
                # print(deleted_keys)

if __name__ == "__main__":
   # demo_node()
    #demo_tree()
    demo_treeNum()








