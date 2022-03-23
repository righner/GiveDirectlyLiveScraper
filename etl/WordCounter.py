# wget https://raw.githubusercontent.com/rainermensing/GDLive-Explorer/main/streamlit.py

import pickle
import nltk
nltk.download(['averaged_perceptron_tagger'])
import pandas as pd
import numpy as np
from etl.gbq_functions import get_aggregate_data

from dask import delayed, compute
from dask.diagnostics import ProgressBar
pbar = ProgressBar()
pbar.register()

def pickle_full_sample_count():
    df = get_aggregate_data()
    text = " ".join(response for response in df.agg_response)
    noun_df,verb_df,adj_df = WordCounter(text)
    with open('full_sample_count.pkl', 'wb') as file:
        pickle.dump([noun_df,verb_df,adj_df], file)

def WordCounter(text): 


    #print('PROPER NOUNS EXTRACTED :')
    noun_list = []
    verb_list = []
    adj_list = []
    
    

    temp = np.char.replace(text,"."," ") # Use instead of nltks slow sent_tokenize()
    temp = np.char.replace(temp,","," ") #remove commas
    temp = np.char.replace(temp,"!"," ") #remove exclamations marks
    clean_text = np.char.replace(temp,"?"," ") #remove question marks    
    words = str.split(str(clean_text)) #split text into single words #FYI using np.char.split here caused an Index Error when trying to split the np array into partitions using np.array_split.
    print("Text cleaned and split")

    tasks = []
    split_array = np.array_split(words,15) #A test on the full dataset showed that 15 parallel dask tasks are optimal.
    for array in split_array:
        task = delayed(nltk.pos_tag)(array)
        tasks.append(task)        

    dask_product = compute(*tasks)
     #creating a numpy version of nltks' tagger funtion
    print("Words tagged")
    for tagged in dask_product:
        for (word, tag) in tagged:
            if tag in ['NN','NNP','NNS']: # If the word is a noun
                noun_list.append(word)
            elif tag in ['VB','VBD','VBG','VBN','VBP','VBZ']: # If the word is a verb
                verb_list.append(word)
            elif tag in ['JJR','JJS']: # If the word is an adjective
                adj_list.append(word)    
        noun_df = pd.DataFrame(noun_list,columns=["Noun"]).groupby(["Noun"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)
        verb_df = pd.DataFrame(verb_list,columns=["Verb"]).groupby(["Verb"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)
        adj_df = pd.DataFrame(adj_list,columns=["Adjective"]).groupby(["Adjective"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)
        
    #Let index start from 1 instead of 0
    noun_df.index = noun_df.index + 1
    verb_df.index = verb_df.index + 1
    adj_df.index = adj_df.index + 1
    print("Wordcount complete")

    return noun_df,verb_df,adj_df

if __name__ == '__main__':
    pickle_full_sample_count()
