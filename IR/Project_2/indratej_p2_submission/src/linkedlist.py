'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import math
import numpy as np

class Node:

    def __init__(self, value=None, next=None, skip_next=None, tf=0):
        """ Class to define the structure of each node in a linked list (postings list).
            Value: document id, Next: Pointer to the next node
            Add more parameters if needed.
            Hint: You may want to define skip pointers & appropriate score calculation here"""
        self.value = value #document-ID
        self.next = next #next pointer
        self.skip_next = skip_next #skip pointer
        self.tf = tf #tf score for each term,doc pair
        
    def increase_tf_count(self):
        self.tf = self.tf+1


class LinkedList:
    """ Class to define a linked list (postings list). Each element in the linked list is of the type 'Node'
        Each term in the inverted index has an associated linked list object.
        Feel free to add additional functions to this class."""
    def __init__(self):
        self.start_node = None #start of PL
        self.end_node = None #end of PL
        self.length, self.n_skips = 0, 0 #length of PL, no of skips, term freq
        self.skip_length = None #skip length
        
    def traverse_list(self):
        
        if self.start_node is None:
            return ["List is empty"]
        
            """ Write logic to traverse the linked list.
                To be implemented."""
        traversal = []
        tf_scores = []
        curr = self.start_node
        traversal.append(curr.value)
        tf_scores.append(curr.tf)
        while(curr.next):
            traversal.append(curr.next.value)
            tf_scores.append(curr.next.tf)
            curr = curr.next
        return traversal,tf_scores

    def traverse_skips(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list using skip pointers.
                To be implemented."""
            curr = self.start_node
            if(curr.skip_next):
                traversal.append(curr.value)
            while(curr.skip_next):
                traversal.append(curr.skip_next.value)
                curr = curr.skip_next
            return traversal

    def add_skip_connections(self):
        if(self.length < 3):
            return
        
        """ Write logic to add skip pointers to the linked list. 
            This function does not return anything.
            To be implemented."""
        
        n_skips = math.floor(math.sqrt(self.length))
        if n_skips * n_skips == self.length:
            n_skips = n_skips - 1
        
        self.n_skips = n_skips
        self.skip_length = int(np.round(math.sqrt(self.length),0))
        
        prev = None
        curr = self.start_node
        while(curr):
            count = 0
            prev = curr
            while((curr.next is not None) & (count < self.skip_length)):
                curr = curr.next
                count += 1
            if(count == self.skip_length):
                prev.skip_next = curr 
            if(curr.next is None):
                break

    def insert(self, value):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        new_node = Node(value,None,None,1)
        prev_node = None
        curr_node = self.start_node
        
        if(self.length == 0 or curr_node is None):
            self.start_node = self.end_node = new_node
        else:
            while((curr_node.next is not None) & (curr_node.value < value)):
                prev_node = curr_node
                curr_node = curr_node.next
            if(curr_node.value > value):
                if(prev_node is None):
                    self.start_node = new_node
                    self.start_node.next = curr_node
                else:
                    prev_node.next = new_node
                    new_node.next = curr_node
            elif(curr_node.value == value):
                curr_node.increase_tf_count()
                self.length -= 1
            else :
                new_node.next = curr_node.next
                curr_node.next = new_node
                if(new_node.next is None):
                    self.end_node = new_node
        self.length += 1  

