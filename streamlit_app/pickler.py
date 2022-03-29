import pickle


def pickle_data(pickle_id,*args):
    """
    Pickles one or more items fromfrom memory to a file for later use.
    
    Parameters
    ----------
    filter_id : str
        An ID string 
    args: list
        list of one or more data variables
    
    """ 
    
    with open(pickle_id+".pickle", "wb") as f:
        pickle.dump(len(args), f)
        for value in args:
            pickle.dump(value, f)

def read_pickled_data(pickle_id):
    """
    Loads pickled data based on ID string. 
    
    Parameters
    ----------
    filter_id : str
        An ID strong made of the month of the request and the a hash-value generated from the filter settings. 

    Returns
    -------
    data: list
        A list containing one or more files

    """ 
    data = []
    with open(pickle_id+".pickle", "rb") as f:
        for _ in range(pickle.load(f)):
            data.append(pickle.load(f))
    return data #list of dataframes
