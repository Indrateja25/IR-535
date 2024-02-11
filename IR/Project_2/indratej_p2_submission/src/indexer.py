'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from linkedlist import LinkedList
from collections import OrderedDict
import math
import numpy as np

class Indexer:
    def __init__(self):
        """ Add more attributes if needed"""
        self.inverted_index = OrderedDict({}) #contains key:term, value:(length of PL, PL)
        self.doc_token_counts = OrderedDict({}) #no of tokens in a document 
        self.tf_idf_scores = OrderedDict({}) #tf-idf scores for each (term,doc) pair
        
    def get_index(self):
        """ Function to get the index.
            Already implemented."""
        return self.inverted_index
    
    def get_doc_token_counts(self):
        """ Function to get the index.
            Already implemented."""
        return self.doc_token_counts

    def get_tf_idf_scores(self):
        """ Function to get the index.
            Already implemented."""
        return self.tf_idf_scores
    
    def generate_inverted_index(self, doc_id, tokenized_document):
        """ This function adds each tokenized document to the index. This in turn uses the function add_to_index
            Already implemented."""
        for term_ in tokenized_document:
            self.add_to_index(term_, doc_id)
        self.doc_token_counts[doc_id] = len(tokenized_document)

    def add_to_index(self, term_, doc_id_):
        
        """ This function adds each term & document id to the index.
            If a term is not present in the index, then add the term to the index & initialize a new postings list (linked list).
            If a term is present, then add the document to the appropriate position in the posstings list of the term.
            To be implemented."""
        if term_ not in self.inverted_index.keys():
            self.inverted_index[term_] = [0,LinkedList()]
            
        lpl = self.inverted_index[term_][0]
        self.inverted_index[term_][0] = lpl + 1
        self.inverted_index[term_][1].insert(doc_id_)

    def sort_terms(self):
        """ Sorting the index by terms.
            Already implemented."""
        sorted_index = OrderedDict({})
        for k in sorted(self.inverted_index.keys()):
            sorted_index[k] = self.inverted_index[k]
        self.inverted_index = sorted_index

    def add_skip_connections(self):
        """ For each postings list in the index, add skip pointers.
            To be implemented."""
        for term_ in self.inverted_index.keys():
            self.inverted_index[term_][1].add_skip_connections()

    def calculate_tf(self,n_term,n_doc):
        return n_term/n_doc
            
    def calculate_idf(self, D, N):
        if D == 0:
            return 0 
        return math.log(N / D)
            
    def calculate_tf_idf(self):
        """ Calculate tf-idf score for each document in the postings lists of the index.
            To be implemented."""
        tf_idf_scores = {}
        N = len(self.doc_token_counts)
        for term_ in self.inverted_index.keys():
            D = self.inverted_index[term_][0]
            idf_score = self.calculate_idf(D,N)
            try:
                PL,tf_counts = self.inverted_index[term_][1].traverse_list() 
                for i in range(len(PL)):
                    n_term =  tf_counts[i]
                    n_doc = self.doc_token_counts[PL[i]]
                    tf_score = self.calculate_tf(n_term,n_doc)
                    tf_idf_scores[(term_,PL[i])] = np.round(tf_score * idf_score, 2)
            except Exception as e:
                print("Exception" ,e)
        self.tf_idf_scores = tf_idf_scores
