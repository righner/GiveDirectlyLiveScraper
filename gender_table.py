from gbq_functions import get_names, load_gender_table

def create_gender_table(gender_api_key):
    table_df = get_names()
    gender_api_key = "GQfxJNVTUPuusuknko"
    table_df[["gender","accuracy","sample_size"]] = table_df.apply(get_gender,gender_api_key,axis=1, result_type ='expand')


    load_gender_table(table_df)

def get_gender(row,gender_api_key):
    from json import loads
    from urllib.request import urlopen
    print(row.name)
    api_url = "https://gender-api.com/get?key=" + gender_api_key + "&name=" + row["name"]
    response = urlopen(api_url)
    decoded = response.read().decode('utf-8')
    data = loads(decoded)
    gender = data["gender"]
    accuracy = data["accuracy"]
    sample = data["samples"]

    return gender, accuracy, sample
