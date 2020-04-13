from random import choice, randint, sample, seed

# My implementation of a bplus tree for database entry indexing


### Node implementation for the bplus tree ###
# :brief        #The leaf nodes keep track of the left and right neighbors 
#               #Internal nodes will have left and right set to None 
#               #Keys are the values of the entries are indexed on and 
#               #RIDs correspond to the id of a single entry 
#               #In the internal nodes, the rids list will be a list of children 


class Node():
    ### Initializer function for nodes ###
    # :param max_node_size: int      #Max number of keys per node
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
    
    # Sets the is_leaf and will change left and right to none if is_leaf is set to none
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

    ### Adds a key and rid pair to a node ###
    # :param key:           #key to add to the tree
    # :param rid: int       #RID the corresponds to entry with key value
    # :brief    :           #Compares key value to determine were to add within node
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
        
    ### Removes a key and rid pair from a node ###
    # :param key:       #key to add to the tree
    # :param rid:       #RID the corresponds to entry with key value
    # :brief    :
    #                   #Goes through the keys of the node and if it finds the key
    #                   #Will traverse the corresponding rids until it finds the right rid or not
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

    # Reorders the keys values of the node after a tree balancing 
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


    ### splits a node ###
    # :brief    :       #Creats a left and right child node from original node.
                        #Moves the keys and rids of the calling node to the new nodes.
                        #Sets left and right as the children of the calling node and changes is_leaf to false.
                        #Diffferent splitting protocal depending on max node size parity
    def split(self):
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

    # Node is full if it has more than max node number
    def is_full(self):
        return len(self.keys) == self.max_node_size + 1

    ### Search function to key location ###
    # :param key:           #Key to search for in node
    # :return RID, index:   #Returns a RID or Node and its index
    def _find(self, key): 
        for i, item in enumerate(self.keys):
            if key < item:
                return self.rids[i], i
                
        if i < len(self.rids) - 1:
            return self.rids[i+1], i+1
        else:
            return None, i

    # Recursive function to print out a node and its descendents
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

    # Calls key_helper to generate a string of the current node and all its children
    def get_keys(self):
        bool = []
        to_print = self.key_helper(bool)
        return to_print
            
### Implementation of the Bplus tree ###
class BPlusTree(object): 

    ### Initializer function ###
    # :param max_node_size: int     #Max number of keys per node    
    # :brief    :                   #Takes an int for max node size and creates a root node in initilization
    #                               #Sets leftmost node and rightmost node of tree to root node
    def __init__(self, max_node_size = 4):
        self.root = Node(max_node_size)
        self.left = self.root
        self.right = self.root

    ### Merges a child node with its parent (after a split) ###
    # :param parents:   #List of parent nodes and index of child nodes (allows traversal up the tree)
    # :param child:     #Node that was just split
    # :brief    :       #Pops childs closest ancestor (direct parent) and child's index
    #                   #Iterates through the parent's keys and adds the child key to them
    #                   #If this makes the parent full, it will call split on the parent node
    #                   #If parent splits, recursively call on parent to merge up tree
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
                self._merge(parents, parent)

    ### Inserts a key and RID pair to the tree ###
    # :param key:       #Key to add to tree 
    # :param RID:       #RID corresponding to key
    # :brief    :       #Finds to leaf node for the key provided
    #                   #If the key exists it will add rid to the key's bucket
    #                   #Otherwise it will add the key and rid pair to node
    #                   #Will call split if the node is full and merge it with it's parent
    def insert(self, key, rid):

        child = self.root
        prev_node = self.root
        parents = []

        while not child == None and not child.is_leaf:

            child, index = child._find(key)
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

    ### Removes a key and rid pair from the tree ###
    # :param key:       #Key to find in tree
    # :param rid:       #RID to remove from tree
    # :brief    :       #If removing the rid results in the key having no corresponding rids
    #                   #Will delete the key from the leaf node
    #                   #Rebalances removal results in # of keys less than 1/2 max_node_size
    def remove(self, key, rid):

        parents = []
        prev_node = self.root
        child = self.root

        while not child == None and not child.is_leaf:
            child, index = child._find(key)
            parents.append((prev_node, index))
            prev_node = child

        if child != None:
            child.remove(key,rid)
            
            if(len(child.keys) < child.max_node_size/2):
                self._balance(parents, child)

        return None

    ### Finds node by rid by traversing the leaf nodes ###
    # :param rid:       #RID to remove from tree
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
        
    ### Balances the the tree after a key is removes ###
    # :param parents:   #List of parent nodes and index of child nodes (allows traversal up the tree)
    # :param child:     #child node is a node that with less than max_node_num/2 # of keys
    # :brief    :       #Pops child's closest ancestor (direct parent) and child's index
    #                   #Finds the left sibling of the child if the child is not the rightmost node of parent
    #                   #Will combine or borrow from neighbor depending of the # of keys in neighbor node
    #                   #If two nodes combining results in the parent node with less than ax_node_num/2 # of keys
    #                   #Will recursively call balance on the parent and balance up the tree
    def _balance(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
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
        indices = (index, sibling_index)

        if len(sibling.keys) <= (sibling.max_node_size + 1)/2:
            self.consolidate(indices, child, sibling, parent, flags)

            if parent != self.root and len(parent.keys) < parent.max_node_size/2 :
                self._balance(parents, parent)
            elif parent == self.root and not parent.keys:
                if not parent.keys:
                    self.root = parent.rids[0]

        else:
            self.share(indices, child, sibling, parent, flags)

    ### Helper function for balancing that combines two nodes ###
    # :param indices:   #Tuple of ndices of child and sibling
    # :param child:     #Node the needs to have more keys
    # :param sibiling:  #Node that child combining with
    # :param parent:    #Parent node of child and sibling nodes
    # :param flag:      #Tuple of two bools that indicate
    #                   #Bool 1 for if sibling is to the left
    #                   #Bool 2 for if child is a leaf node
    # :brief    :       #Called if the sibling has less than or equal to (max node size + 1)/2
    #                   #Moves all the sibling keys to the child keys and updates the parents keys if needed

    def consolidate(self, indices, child, sibling, parent, flags):
        index, sibling_index = indices
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

    ### Helper function for balancing that moves keys from sibling to child ###
    # :param indices:   #Tuple of ndices of child and sibling
    # :param child:     #Node the needs to have more keys
    # :param sibiling:  #Node that child combining with
    # :param parent:    #Parent node of child and sibling nodes
    # :param flag:      #Tuple of two bools that indicate
    #                   #Bool 1 for if sibling is to the left
    #                   #Bool 2 for if child is a leaf node
    # :brief    :       #Called if the sibling has more than (max node size + 1)/2
    #                   #Moves the sibling keys to the child keys until child has max_node_size/2 keys or more
    #                   #Will updates the parents keys if needed

    def share(self, indices, child, sibling, parent, flags):
        index, sibling_index = indices
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

    ### Search function by range ###
    # :param start:     #Start value of range
    # :param end:       #End value of range
    # :brief    :       #Finds the start of the range and then traverses the leaf nodes
    # :return list:     #List of rids in the range given
    def bulk_search(self, start, end):

        current_node = self.root
        while not current_node == None and not current_node.is_leaf:
            current_node, index = current_node._find(start)
        
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

    ### Sum function by range ###
    # :param start:     #Start value of range
    # :param end:       #End value of range
    # :brief    :       #Finds the start of the range and then traverses the leaf nodes
    # :return int:      #Sum of keys in the range given
    def sum_range(self, start, end):
        current_node = self.root
        while not current_node == None and not current_node.is_leaf:
            current_node, index = current_node._find(start)
        
        sum = 0
        current_key = start
        
        while current_key <= end and current_node != None:

            for i, key in enumerate(current_node.keys):

                if key >= current_key and key <= end:
                    current_key = key

                    for rid in current_node.rids:
                        sum += current_key
            
            current_node = current_node.right
                    
        return sum
    
    # Returns all the leaf nodes as a list with root node at the beginning (for writting to disk)
    def get_all_leaves(self):

        current_node = self.left
        return_data = [self.root.keys]

        while current_node != None:
            return_data.append((current_node.keys,current_node.rids))
            print(current_node.keys,"next")
            current_node = current_node.right

        return return_data

    ### Finds rids by keys ###
    # :param key:       #Key value to search for in tree
    # :return list:     #list of rid(s)
    def get_rid(self, key):
        child = self.root

        while not child == None and not child.is_leaf:
            child, index = child._find(key)

        if child != None:
            for i, item in enumerate(child.keys):
                if key == item:
                    return child.rids[i]

        return None
    
    # Gets tree in string form
    def get_keys(self):
        return self.root.get_keys()

# Test stuff

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








