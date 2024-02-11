'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedlist import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time
import random
import flask
from flask import Flask
from flask import request
import hashlib

app = Flask(__name__)


class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()

    def _merge(self, pl_1, pl_2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        i,j,m,n = 0,0,len(pl_1),len(pl_2)
        pl_merged = []
        num_comparisions = 0
        while((i < m) & (j < n)):
            num_comparisions += 1
            if(pl_1[i] == pl_2[j]):
                pl_merged.append(pl_1[i])
                i += 1
                j += 1
            elif(pl_1[i] < pl_2[j]):
                i += 1
            else:
                j += 1
        return pl_merged,num_comparisions

    def _daat_and(self, terms, score=0):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        terms = list(set(terms))
        PL_values,PL_counts = [],[]
        for term in terms:
            PL,_ = self._get_postings(term)
            PL_values.append(PL)
            PL_counts.append(len(PL))
            
        all_PL = list(zip(PL_counts, PL_values))
        all_PL_sorted = sorted(all_PL, key=lambda x: x[0])
        PL_counts, PL_values  = zip(*all_PL_sorted)

        min_PL = PL_values[0]
        total_comparisions = 0
        for i in range(1,len(PL_values)):
            min_PL,num_comparisions = self._merge(min_PL, PL_values[i])
            total_comparisions += num_comparisions

        if((score) & (len(min_PL) > 1)):
            doc_scores = []
            for doc in min_PL:
                total_score = 0
                for term in terms:
                    s = self.indexer.get_tf_idf_scores()[(term,doc)]
                    total_score += s 
                doc_scores.append(total_score)
            answer_score = list(zip(min_PL, doc_scores))
            answer_score_sorted = sorted(answer_score, key=lambda x: x[1],reverse=True)
            answer_score, doc_scores  = zip(*answer_score_sorted)  
            return answer_score,total_comparisions
    
        return min_PL,total_comparisions


    def _get_postings(self,term):
        """ Function to get the postings list of a term from the index.
            Use appropriate parameters & return types.
            To be implemented."""
        return self.indexer.get_index()[term][1].traverse_list()

    def _merge_skip(self, pl_1, pl_2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        pl_merged = []
        num_comparisions = 0
        while((pl_1 is not None) & (pl_2 is not None)):
            num_comparisions += 1
            if(pl_1.value == pl_2.value):
                pl_merged.append(pl_1.value)
                pl_1 = pl_1.next
                pl_2 = pl_2.next
                
            elif(pl_1.value < pl_2.value):
                if(pl_1.skip_next is not None) and (pl_1.skip_next.value <= pl_2.value):
                    while(pl_1.skip_next is not None) and (pl_1.skip_next.value <= pl_2.value):
                        pl_1 = pl_1.skip_next
                else:
                    pl_1 = pl_1.next
                  
            else:
                if(pl_2.skip_next is not None) and (pl_2.skip_next.value <= pl_1.value):  
                    while(pl_2.skip_next is not None) and (pl_2.skip_next.value <= pl_1.value):
                        pl_2 = pl_2.skip_next
                else:
                    pl_2 = pl_2.next
                    
        return pl_merged,num_comparisions
    
    def _daat_and_skip(self, terms, score=0):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        terms = list(set(terms))
        start_ptrs,PL_counts = [],[]
        for term in terms:
            p = self.indexer.get_index()[term][1].start_node
            l = self.indexer.get_index()[term][1].length
            start_ptrs.append(p)
            PL_counts.append(l)
            
        all_PL = list(zip(PL_counts, start_ptrs))
        all_PL_sorted = sorted(all_PL, key=lambda x: x[0])
        PL_counts, start_ptrs  = zip(*all_PL_sorted)

        min_start_ptr = start_ptrs[0]
        total_comparisions = 0
        for i in range(1,len(start_ptrs)):
            answer,num_comparisions = self._merge_skip(min_start_ptr, start_ptrs[i])
            total_comparisions += num_comparisions
            
        if((score) & (len(answer) > 1)):
            doc_scores = []
            for doc in answer:
                total_score = 0
                for term in terms:
                    total_score += self.indexer.get_tf_idf_scores()[(term,doc)]
                doc_scores.append(total_score)
            answer_score = list(zip(answer, doc_scores))
            answer_score_sorted = sorted(answer_score, key=lambda x: x[1],reverse=True)
            answer_score, doc_scores  = zip(*answer_score_sorted) 
            return answer_score,total_comparisions

        return answer,total_comparisions
    

    def _output_formatter(self, op):
        """ This formats the result in the required format.
            Do NOT change."""
        if op is None or len(op) == 0:
            return [], 0
        op_no_score = [int(i) for i in op]
        results_cnt = len(op_no_score)
        return op_no_score, results_cnt

    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        try:
            with open(corpus, 'r') as fp:
                for line in tqdm(fp.readlines()):
                    doc_id, document = self.preprocessor.get_doc_id(line)
                    tokenized_document = self.preprocessor.tokenizer(document)
                    self.indexer.generate_inverted_index(doc_id, tokenized_document)
                self.indexer.sort_terms()
                self.indexer.add_skip_connections()
                self.indexer.calculate_tf_idf()
        except Exception as e:
                print("Exception" ,e)

    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}

    def run_queries(self, query_list, random_command):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': {}#self.sanity_checker(random_command)
                      }

        for query in tqdm(query_list):
            """ Run each query against the index. You should do the following for each query:
                1. Pre-process & tokenize the query.
                2. For each query token, get the postings list & postings list with skip pointers.
                3. Get the DAAT AND query results & number of comparisons with & without skip pointers.
                4. Get the DAAT AND query results & number of comparisons with & without skip pointers, 
                    along with sorting by tf-idf scores."""
            
            input_term_arr = self.preprocessor.tokenizer(query)  # Tokenized query. To be implemented.

            for term in input_term_arr:
                postings,scores = self.indexer.get_index()[term][1].traverse_list()
                skip_postings = self.indexer.get_index()[term][1].traverse_skips()


                """ Implement logic to populate initialize the above variables.
                    The below code formats your result to the required format.
                    To be implemented."""

                output_dict['postingsList'][term] = postings
                output_dict['postingsListSkip'][term] = skip_postings

            and_op_no_skip,and_comparisons_no_skip = self._daat_and(input_term_arr,score=0)
            and_op_skip,and_comparisons_skip = self._daat_and_skip(input_term_arr,score=0) 
            and_op_no_skip_sorted,and_comparisons_no_skip_sorted = self._daat_and(input_term_arr,score=1) 
            and_op_skip_sorted,and_comparisons_skip_sorted = self._daat_and_skip(input_term_arr,score=1) 
            
            """ Implement logic to populate initialize the above variables.
                The below code formats your result to the required format.
                To be implemented."""
            and_op_no_score_no_skip, and_results_cnt_no_skip = self._output_formatter(and_op_no_skip)
            and_op_no_score_skip, and_results_cnt_skip = self._output_formatter(and_op_skip)
            and_op_no_score_no_skip_sorted, and_results_cnt_no_skip_sorted = self._output_formatter(and_op_no_skip_sorted)
            and_op_no_score_skip_sorted, and_results_cnt_skip_sorted = self._output_formatter(and_op_skip_sorted)
            
            output_dict['daatAnd'][query.strip()] = {}
            output_dict['daatAnd'][query.strip()]['results'] = and_op_no_score_no_skip
            output_dict['daatAnd'][query.strip()]['num_docs'] = and_results_cnt_no_skip
            output_dict['daatAnd'][query.strip()]['num_comparisons'] = and_comparisons_no_skip

            output_dict['daatAndSkip'][query.strip()] = {}
            output_dict['daatAndSkip'][query.strip()]['results'] = and_op_no_score_skip
            output_dict['daatAndSkip'][query.strip()]['num_docs'] = and_results_cnt_skip
            output_dict['daatAndSkip'][query.strip()]['num_comparisons'] = and_comparisons_skip

            output_dict['daatAndTfIdf'][query.strip()] = {}
            output_dict['daatAndTfIdf'][query.strip()]['results'] = and_op_no_score_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_docs'] = and_results_cnt_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_no_skip_sorted

            output_dict['daatAndSkipTfIdf'][query.strip()] = {}
            output_dict['daatAndSkipTfIdf'][query.strip()]['results'] = and_op_no_score_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_docs'] = and_results_cnt_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_skip_sorted

        return output_dict


@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)


if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()

    corpus = argv.corpus
    output_location = argv.output_location
    username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)

    app.run(host="0.0.0.0", port=9999)
