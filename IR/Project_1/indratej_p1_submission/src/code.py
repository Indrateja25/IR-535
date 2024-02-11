import wikipedia
import pysolr
import os
import numpy as np
import pandas as pd
import requests

search_topics = ['Health', 'Environment', 'Technology', 'Economy', 'Entertainment',
                 'Sports', 'Politics', 'Education', 'Travel', 'Food']

min_documents = 550
max_short_summaries = min_documents*0.05
core_name = 'IRF23P1'
url = 'http://35.188.149.137:8983/solr/'

#config
def delete_core(core_name):
    print(os.system('sudo -u solr ./solr-9.0.0/bin/solr delete -c {core}'.format(core=core_name)))


def create_core(core_name):
    print(os.system('sudo -u solr ./solr-9.0.0/bin/solr create -c {core} -n data_driven_schema_configs'.format(
            core=core_name)))
       
#scrape
def check_page_validity(page,csr):
    page_content = wikipedia.page(page, auto_suggest=False,redirect=True, preload=False)
    page_summary = page_content.summary
    page_summary  = remove_non_ascii_chars(page_summary)

    if(len(page_summary) >= 200):
        return 2
    if(len(page_summary) < 200 and csr <=  max_short_summaries):
        return 1
    return 0

def scrape_helper(page_titles, topic):
    topic_docs_json = []
    
    for page_title in page_titles:
        page_content = wikipedia.page(page_title, auto_suggest=False,redirect=True, preload=False)    
        topic_docs_json.append(
            {
            'revision_id':page_content.revision_id,
            'title': page_content.title,
            'summary':page_content.summary,
            'url': page_content.url,
            'topic':topic
            }
        )
    return topic_docs_json

def scrape_topic_wikipedia(topic,min_documents):
    subtopic_page_titles = set()
    count_short_sumaries = 0
    
    
    subtopic_searches = wikipedia.search(topic,min_documents)  
    for subtopic in subtopic_searches:
        try:
            subtopic_content = wikipedia.page(subtopic, auto_suggest=False,redirect=True, preload=False)
            subtopic_page_links = subtopic_content.links
            if(len(subtopic_page_titles) >= min_documents):
                    break
            for page in subtopic_page_links:
                if(len(subtopic_page_titles) >= min_documents):
                    break
                val = check_page_validity(page,count_short_sumaries)
                if(val > 0):
                    subtopic_page_titles.add(page)
                    if(val == 1):
                        count_short_sumaries += 1
                l = len(subtopic_page_titles)
                if(l % 20 == 0):
                    print("subtopics finding completed : ",round(100*l/min_documents,2),"%")
        except Exception as e:
            #print(e)
            1+1
            
    print("scraping individual pages..")
    page_titles = list(subtopic_page_titles)[:min_documents]
    scraped_data = scrape_helper(page_titles,topic)
    
    return scraped_data

#preprocess

def remove_non_ascii_chars(text):
    return ''.join([c if ord(c) < 128 or c.isspace() or c.isdigit() else '' for c in text])

def preprocess_summary(df,column):
    df[column] = df[column].replace(r"[^\w\s]", "", regex=True)
    df[column] = df[column].apply(str).apply(remove_non_ascii_chars)
    return df

#index
def get_schema():
    schema = {
            "add-field": [
                {
                    "name": "revision_id",
                    "type": "string",
                    "indexed": True,
                    "multiValued": False
                },
                {
                    "name": "title",
                    "type": "string",
                    "indexed": True,
                    "multiValued": False
                },
                {
                    "name": "summary",
                    "type": "text_en",
                    "indexed": True,
                    "multiValued": False
                },
                {
                    "name": "url",
                    "type": "string",
                    "indexed": False,
                    "multiValued": False
                },
                {
                    "name": "topic",
                    "type": "string",
                    "indexed": True,
                    "multiValued": False
                },
            ]
        }
    
    return schema

def set_config():
    delete_core(core_name)
    create_core(core_name)
    solr = pysolr.Solr(url+core_name, always_commit=True, timeout=10)
    
    print("creating required schema")
    schema = get_schema()
    requests.post(url+core_name + "/schema", json=schema).json()
    
    return solr

#main
def main():
    try:
        #scrape
        all_topics_list = []
        for topic in search_topics: 
            print("finding page titles for TOPIC:",topic)
            json_list = scrape_topic_wikipedia(topic,min_documents)
            all_topics_list.append(json_list)


        json_object = np.array(all_topics_list) 
        json_object = json_object.flatten()
        df = pd.json_normalize(json_object)

        #preprocess
        df = preprocess_summary(df,'summary')

        #save to local
        df.to_csv('./indexed_data.csv',index=False)

        #index
        collection = df.to_dict('records')
        solr = set_config()
        solr.add(collection)
	
        print("indexed ",df.shape[0]," documents in total to core:",core_name)        

    except Exeption as e:
        print(e)
    
main()
