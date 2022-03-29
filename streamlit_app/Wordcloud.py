import streamlit as st
import matplotlib.pyplot as plt
from wordcloud import WordCloud


def create_Wordcloud(text, max_word, max_font, random):
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
    
    cloud = WordCloud(mode = "RGBA",background_color=None, max_words=max_word,
    max_font_size=max_font, random_state=random,width=1600, height=900)

    # generate word cloud
    cloud.generate(text)

    return cloud


def plot_Wordcloud(cloud):
    """
    Takes a WordCloud, renders it into an image using matplotlib and plots it via st.pyplot. 

    Parameters:
    cloud: WordCloud
        A generated WordCloud.   

    """
    fig, ax = plt.subplots() #creates a figure and a grid of subplots with a single call
    ax.imshow(cloud) #render wc as image
    plt.axis('off')    
    st.pyplot(fig) #plot figure on streamlit
    