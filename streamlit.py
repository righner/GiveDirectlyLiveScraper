import matplotlib.pyplot as plt
import streamlit as st
from wordcloud import WordCloud

import nltk
nltk.download(['averaged_perceptron_tagger','punkt'])
import pandas as pd

from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache(ttl = 86400, show_spinner = False) #Cache df for 24h
def get_aggregate_data():
    query = """
    SELECT * 
    FROM `gdliveproject.tests.GDLive_aggregate`
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    df = job.result().to_dataframe()
    return df[df['usdollar'] < 1000] # Filter entries with wrong payment amount

def filter_df(df, gender, question,campaign,enrollments,min_amount,max_amount):
    if enrollments == False:
        df = df.dropna(subset="usdollar")
        df = df[df['usdollar'].isin(range(min_amount-1,max_amount+1))]
    if len(gender) != 0:
        df = df[df['gender'].isin(gender)]
    if len(question) != 0:
        df = df[df['question'].isin(question)]
    if len(campaign) != 0:
        df = df[df['campaign'].isin(campaign)]
    return df


def text_from_filter(df,*args):
    
    text = " ".join(response for response in df.agg_response)

    print("Aggregate ready for analysis")
    return text

def cloud(text, max_word, max_font, random):
    
    wc = WordCloud(mode = "RGBA",background_color=None, max_words=max_word,
    max_font_size=max_font, random_state=random,width=1600, height=900)

    # generate word cloud
    wc.generate(text)

    # create coloring from image

    # show the figure
    plt.figure(figsize=(16,9))
    fig, ax = plt.subplots()
    ax.imshow(wc)
    plt.axis('off')
    st.pyplot(fig)

def WordCounter(text):


    #print('PROPER NOUNS EXTRACTED :')
    noun_list = []
    verb_list = []
    adj_list = []

    sentences = nltk.sent_tokenize(text)
    for sentence in sentences:
        words = nltk.word_tokenize(sentence)
        tagged = nltk.pos_tag(words)
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

    return noun_df,verb_df,adj_df

def sizable_text(px,text):
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
        agg_df = get_aggregate_data()

    st.write("## Step I: Set the filter")
    gender = st.multiselect("Gender",agg_df["gender"].unique())
    question = st.multiselect("Question",agg_df["question"].unique())
    campaign = st.multiselect("Campaign",agg_df["campaign"].unique())
    amount_range = agg_df.sort_values("usdollar")["usdollar"].dropna().unique().astype(int)
    min_amount,max_amount = st.select_slider("Payout in USD", options=amount_range,value=(1,amount_range.max()))
    #min_amount,max_amount = (0,1000)

    if min_amount > 1 or max_amount < amount_range.max():
        enrollments = False #st.checkbox("Include enrollment surveys (without transfer)", value= False,disabled=True)
        st.warning("Filtering by amount will exclude enrollment surveys, since they have no payment.")
    else:
        enrollments = True #st.checkbox("Include enrollment surveys (without transfer)", value= True)

    st.write("## Step II: Create a Wordcloud")
    with st.expander("Optional: Configurate Wordcloud"):
        max_word = st.slider("Max words", 5, 1000, 200)
        max_font = st.slider("Max Font Size", 50, 350, 150)
        random = st.slider("Random State", 30, 100, 42 )

    try:
        if st.button("Create Wordcloud",key = "cloud"):
            with st.spinner("Creating Wordcloud..."):
                filtered_df = filter_df(agg_df,gender,question,campaign,enrollments,min_amount,max_amount)    
                text = text_from_filter(filtered_df)
                # st.image(image, width=100, use_column_width=True)
                st.write(cloud(text, max_word, max_font, random), use_column_width=True)
        
        else:
            st.info("Hit 'Create Wordcloud' when you are ready")
    except ValueError:
        st.info("The selection you made is too narrow. Please remove or change the filter.")
    
    
    st.write("## Step III: Calculate Wordcount")
    st.warning("This could take a long time (up to 5-10 min with no filters), depending on your filter settings. If you decide to stop a calculation, you likely have to refresh the page to do another analysis.")
    try:
        
        if st.button("Calculate Wordcount",key = "count"):
            with st.spinner("Calculating Wordcount..."):
                filtered_df = filter_df(agg_df,gender,question,campaign,enrollments,min_amount,max_amount)    
                text = text_from_filter(filtered_df)

                nouns, verbs, adjectives = st.columns(3)
                noun_df,verb_df,adj_df = WordCounter(text)
                with nouns:
                    st.write("### Nouns")
                    st.dataframe(noun_df)
                with verbs:
                    st.write("### Verbs")
                    st.dataframe(verb_df)
                with adjectives:
                    st.write("### Adjectives")   
                    st.dataframe(adj_df)
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
