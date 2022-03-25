import pickle
import pandas as pd
objects = []
with open("full_sample_count.pkl", "rb") as f:
    while True:
        try:
            objects = pd.read_pickle(f)
        except EOFError:
            break