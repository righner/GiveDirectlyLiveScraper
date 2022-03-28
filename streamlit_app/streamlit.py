
import streamlit as st

import matplotlib.pyplot as plt
from wordcloud import WordCloud
import hashlib
from datetime import datetime

from dask.diagnostics import ProgressBar
pbar = ProgressBar()
pbar.register()

#Importing other modules
import sys
sys.path.append('./') #Putting other modules on path
from WordCounter import WordCounter, pickle_count, read_pickled_count
from etl.gbq_functions import get_aggregate_data

import os
if os.getcwd() == "/app/gdlive-explorer":  #If on streamlit cloud, get client via streamlit secrets  
    from streamlit_app.streamlit_cloud_client import get_stcloud_client
    client = get_stcloud_client()
else: #get it from the GCP environment
    from google.cloud import bigquery
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "gcp_key.json" #Needed on local machine
    client = bigquery.Client()
    

@st.cache(ttl = 86400, show_spinner = False) #Cache df for 24h
def cache_aggregate_data():
    """
    Calls the get_aggregate_data to extract the aggregate data table, and loads it into the users cache. 
    """
    return get_aggregate_data()


def filter_df(df, gender, question,campaign,no_enrollments,min_amount,max_amount):
    """
    Filters the Pandas DataFrame containing the aggregate data according to serveral parameters given by the user.
    
    Parameters:
    df: Pandas DataFrame
        A pandas DataFrame containing the aggregate data table.    
    gender: list
        List containing the selection of gender options.
    question: list
        List containing the selection of questions.
    campaign: list
        List containing the selection of campaigns.
    no_enrollments: bool
        Boolean value indicating whether enrollment questions should be filtered out.
    min_amount: int
        Integer containing the lower bound of the range of values to be included.
    max_amount: int
        Integer containing the uppr bound of the range of values to be included.

    Returns:
    df: Pandas DataFrame
        A Pandas DataFrame filtered by a set of parameters.
    """
    if no_enrollments:
        df = df.dropna(subset="usdollar")
        df = df[df['usdollar'].isin(range(min_amount-1,max_amount+1))]
    if len(gender) != 0:
        df = df[df['gender'].isin(gender)]
    if len(question) != 0:
        df = df[df['question'].isin(question)]
    if len(campaign) != 0:
        df = df[df['campaign'].isin(campaign)]
    return df


def plot_WordCloud(text, max_word, max_font, random):
    """
    Take a string of text and plots it as a WordCloud. The WordCloud can be customomized according to three parameters. 

    Parameters:
    text: str
        A pandas DataFrame containing the aggregate data table.    
    max_words: int
        Integer setting the max number of words to be diplayed in the WordCloud
    max_font: int
        Integer setting the maximum font size to be used in the WordCloud
    random: int
        Integer setting the a seed foor how to arrange the words.

    """
    
    wc = WordCloud(mode = "RGBA",background_color=None, max_words=max_word,
    max_font_size=max_font, random_state=random,width=1600, height=900)

    # generate word cloud
    wc.generate(text)
    fig, ax = plt.subplots() #creates a figure and a grid of subplots with a single call
    ax.imshow(wc) #render wc as image
    plt.axis('off')
    st.pyplot(fig) #plot figure on streamlit
    

def sizable_text(px,text):
    """
    (Not used here)
    Function that prints markdown text in a custom font size.

    Parameters:
    px: int
        Integer cotaining the font size.    
    text: str
        String with the text to be displayed in markdown format

    """
    st.markdown("""
    <style>
    .font%s {
        font-size:%spx !important;
    }
    </style>
    """%(px,px), unsafe_allow_html=True)

    st.markdown('<p class="font%s">%s</p>'%(px,text), unsafe_allow_html=True)



def main():

    
    st.set_page_config(  # Alternate names: setup_page, page, layout
    page_title = "GDLive Data Explorer",
    layout="wide",  # Can be "centered" or "wide". In the future also "dashboard", etc.
	initial_sidebar_state="auto",  # Can be "auto", "expanded", "collapsed"
    )
    
    st.title("GDLive Data Explorer")
    st.write("Welcome to the unofficial **GDLive Data Explorer**. This dashboard allows you to view text data published on [GiveDirectly](https://www.givedirectly.org/about/)'s [GDLive platform](https://live.givedirectly.org/) in an aggregate format. Since 2009, the NGO GiveDirectly is providing unconditional cash transfers to extremely poor individuals, mostly in Africa. Part of their mission is to show that unconditional cash transfers are a better way to aid people suffering from extreme poverty than most other forms of material aid in most contexts (i.e. whenever it is possible and a market exists locally where the money can be exchanged for goods).")
    st.write("In order to proof this, their projects have been -and continue to be - rigorously evaluated by renown economists using randomized controlled trials (RCTs). The [published results](https://www.givedirectly.org/cash-research-explorer/#) show that the impact of cash transfers is overwhelmingly positive. Especially prejudices that poor individuals will use the 'free money' to buy drugs or alcohol, or that it will create dependencies, could be dismissed early on. Quite the opposite. Recipients often use the funds to invest into the future. Furthermore, there are even so called “general equilibrium effects”, meaning that not only do those benefit that receive the transfers, but also those that live in the same community, since the money is spend and invested locally.")
    st.write("But these studies are not the only way GiveDirectly tries to create transparency about their impact. They also provide raw and unedited survey data from recipients on [GDLive platform](https://live.givedirectly.org/), where respondents answer question like 'How did you spend the money?' and 'How did it change your life?'.")
    st.write("# Exploring the responses")
    st.write("Data in the platform can be viewed profile by profile. But this makes it difficult to get a general overview. This dashboard aims to provide users with a tool to explore these results on an aggregate level, and to filter them by question and relevant categories.")
    st.write("Below, you find a few options with which you can filter the data. You can also just leave the filter blank to see the results for all the data. If you are ready, just click “Apply” to start the analysis.")
    st.info("*Please be aware that this dashboard is still a work in progress. It only uses a sample of less than 10% of profiles on th GDLive platform. Some filter settings could lead to data being based on only very few responses, especially when filtering by question. If you encounter any issues or if you have question, please feel free to reach out to the email in the “About” section below.*")

    with st.spinner("Loading aggregate dataset..."):
        agg_df = cache_aggregate_data()

    st.write("## Step I: Set the filter")
    gender = st.multiselect("Gender",agg_df["gender"].unique())
    question = st.multiselect("Question",agg_df["question"].unique())
    campaign = st.multiselect("Campaign",agg_df["campaign"].unique())
    amount_range = agg_df.sort_values("usdollar")["usdollar"].dropna().unique().astype(int)
    min_amount,max_amount = st.select_slider("Payout in USD", options=amount_range,value=(1,amount_range.max()))
    #min_amount,max_amount = (0,1000)
    
    month = datetime.now().strftime('%Y%m_')    
    filter_hash = hashlib.md5(bytes(str(gender)+str(question)+str(campaign)+str(min_amount)+str(max_amount), 'utf-8')).hexdigest()
    filter_id = month+filter_hash


    if min_amount > 1 or max_amount < amount_range.max():
        no_enrollments = True 
        st.warning("Filtering by amount will exclude enrollment surveys, since they have no payment.")
    else:
        no_enrollments = False 

    st.write("## Step II: Create a Wordcloud")
    with st.expander("Optional: Configurate Wordcloud"):
        max_word = st.slider("Max words", 5, 1000, 200)
        max_font = st.slider("Max Font Size", 50, 350, 150)
        random = st.slider("Random State", 30, 100, 42 )

    try:
        if st.button("Create Wordcloud",key = "cloud"):
            with st.spinner("Creating Wordcloud..."):
                filtered_df = filter_df(agg_df,gender,question,campaign,no_enrollments,min_amount,max_amount)    
                text = " ".join(response for response in filtered_df.agg_response)
                plot_WordCloud(text, max_word, max_font, random)
        
        else:
            st.info("Hit 'Create Wordcloud' when you are ready")
    except ValueError:
        st.info("The selection you made is too narrow. Please remove or change the filter.")
    
    
    st.write("## Step III: Calculate Wordcount")
    try:        
        if st.button("Calculate Wordcount",key = "count"):            
            try:
                with st.spinner("Trying to load cached data..."):
                    noun_df,verb_df,adj_df=read_pickled_count(filter_id)
            except:
                with st.spinner("Calculating Wordcount... This could take a moment, depending on your filter settings (up to two minutes on the unfiltered dataset)."):

                    filtered_df = filter_df(agg_df,gender,question,campaign,no_enrollments,min_amount,max_amount)    
                    text = " ".join(response for response in filtered_df.agg_response)                    
                    noun_df,verb_df,adj_df = WordCounter(text)

            nouns, verbs, adjectives = st.columns(3)
            with nouns:
                st.write("### Nouns")
                st.dataframe(noun_df)
            with verbs:
                st.write("### Verbs")
                st.dataframe(verb_df)
            with adjectives:
                st.write("### Adjectives")   
                st.dataframe(adj_df)
            pickle_count(filter_id,noun_df,verb_df,adj_df)
        else:
            st.write("This will calculate the count of nouns, verbs and adjectives seperately and display them in tables, sorted by frequency.")
            st.info("Hit 'Calculate wordcount' when you are ready")            

    except ValueError:
        st.info("The selection you made is too narrow. Please remove or change the filter.")

    st.write("# Download the aggregate data")
    st.write("You can also download the aggregate data directly. Stopwords (i.e. 'the', 'I', 'a', etc.) have already been removed.")
    st.download_button(
     label="Download data as TSV (tab-seperated-values)",
     data=agg_df.to_csv(sep="\t").encode('utf-8'),
     file_name='GDLive_agg.tsv',
     mime='text/tsv',)	
	
    st.write("# About")
    st.write("If you want to see how this works, visit the projects' [GitHub repository](https://github.com/rainermensing/GDLive-Explorer)\
        Also, please note that this is still work in progress. I am planning to add new features as I have time. Please feel free to reach out to me at [rainer.mensing@hotmail.de](mailto:rainer.mensing@hotmail.de)")
        


            
if __name__=="__main__":
    main()
