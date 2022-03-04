import matplotlib.pyplot as plt
import streamlit as st

def filter_df(df, gender, question,campaign):
    if len(gender) != 0:
        df = df[df['gender'].isin(gender)]
    if len(question) != 0:
        df = df[df['question'].isin(question)]
    if len(campaign) != 0:
        df = df[df['campaign'].isin(campaign)]
    return df

@st.cache
def create_final_df():
    #https://towardsdatascience.com/how-to-clean-text-data-639375414a2f
    import nltk
    nltk.download('stopwords')
    from nltk.corpus import stopwords

    stop_words = stopwords.words('english') + ['money', 'GD', 'first', 'transfer','biggest', 'hardship']
    

    from gbq_functions import get_aggregate_data
    df = get_aggregate_data()

    df['agg_response'] = df['agg_response'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
    print("Stopwords removed")
    return df
 
def text_from_filter(df,*args):
    
    text = " ".join(response for response in df.agg_response)

    print("Aggregate ready for analysis")
    return text

def cloud(text, max_word, max_font, random):
    
    from wordcloud import WordCloud
    wc = WordCloud(mode = "RGBA",background_color=None, max_words=max_word,
    max_font_size=max_font, random_state=random,width=1600, height=900)

    # generate word cloud
    wc.generate(text)

    # create coloring from image

    # show the figure
    plt.figure(figsize=(16,9))
    fig, ax = plt.subplots()
    ax.imshow(wc, interpolation = 'bilinear')
    plt.axis('off')
    st.pyplot(fig)

def WordCounter(text):
    import nltk
    nltk.download('averaged_perceptron_tagger')
    import pandas as pd

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
            elif tag in ['VB','VBD','VBG','VBN','VBP','VBZ']: # If the word is a proper noun
                verb_list.append(word)
            elif tag in ['JJR','JJS']: # If the word is a proper noun
                adj_list.append(word)    
    noun_df = pd.DataFrame(noun_list,columns=["Noun"]).groupby(["Noun"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)
    verb_df = pd.DataFrame(verb_list,columns=["Verb"]).groupby(["Verb"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)
    adj_df = pd.DataFrame(adj_list,columns=["Adjective"]).groupby(["Adjective"]).size().reset_index(name='#').sort_values("#",ascending=False).reset_index(drop=True)

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
    st.write("Welcome to the unofficial **GDLive Data Explorer**. This dashboard allows you to view text data published on GiveDirectly's [GDLIve Plaform](https://live.givedirectly.org/) in an aggregate format. Since about 10 years, the NGO GiveDirectly is providing unconditional cash transfers to extremely poor individuals, mostly in Africa. Part of their mission is to show that unconditional cash transfers are vastly more efficient than most other forms of material aid in most contexts (i.e. whenever it is possible and a market exists locally where  the money can be exchanged for goods).")
    st.write("In order to proof this, their projects have been -and continue to be - rigorously evaluated by renown economists using randomized controlled trials (RCTs). The published results shown so far that the outcome of cash transfers overwhelmingly positive. One stereotype that could be dismissed early on was that poor individuals will not use the money to buy more drugs or alcohol. Quite the opposite. They often allocate the funds with a return on investment in mind. Furthermore, there are even so called “general equilibrium effects”, meaning that not only do those benefit that receive the transfers, but also those that live in the same community, since the money is spend and invested locally.")
    st.write("But these studies are not the only way GiveDirectly tries to create transparency about their impact. They also provide raw survey data from recipients on [GDLIve Plaform](https://live.givedirectly.org/), where respondents answer question like 'How did you spend the money?' and 'How did it change your life?'.")
    st.write("# Exploring the responses")
    st.write("Data in the platform can be viewed profile by profile. But this makes it difficult to get a general overview. This dashboard aims to provide users with a tool to explore these results on an aggregate level, and according to relevant categories.")
    st.write("Below, you find a few options with which you can filter the data. You can also just leave the filter blank to see the results for all the data. If you are ready, just click “Apply” to start the analysis.")
    st.write("*Please be aware that this dashboard is still a work in progress. If you encounter any issues or if you have question, please feel free to reach out to email in the “About” section below.*")

    final_df = create_final_df()
    gender = st.multiselect("Gender",final_df["gender"].unique())
    question = st.multiselect("Question",final_df["question"].unique())
    campaign = st.multiselect("Campaign",final_df["campaign"].unique())

    with st.expander("Optional: Configurate WordCloud"):
        max_word = st.slider("Max words", 5, 1000, 200)
        max_font = st.slider("Max Font Size", 50, 350, 150)
        random = st.slider("Random State", 30, 100, 42 )


        
    
    final_df = filter_df(final_df,gender,question,campaign)
    
    text = text_from_filter(final_df)
    try:
        if st.button("Apply"):
            # st.image(image, width=100, use_column_width=True)
            st.write("## Word cloud")
            st.write(cloud(text, max_word, max_font, random), use_column_width=True)

            st.write("## Word Count")

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
    except ValueError:
        st.info("The selection you made is too narrow. Please remove or change the filter.")
    
    st.write("# About")
    st.write("This Dashboard was build  by me (Rainer) as Capstone project for the Pipeline Academy Data Engineering Bootcamp. If you want to see how it works, visit the projects GitHub repository\
        Also, please note that this is project is still work in progress. I am planning to add new features as I have time. Please feel free to reach out to me at [rainer.mensing@hotmail.de](mailto:rainer.mensing@hotmail.de)")
        


            
if __name__=="__main__":
  main()
