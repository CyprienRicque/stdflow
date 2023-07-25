import pandas as pd


def setup():
    """
    delete all in ./data and create the architecture:
    data
        fr
            step_raw
                v1
                    random.csv
        es
            random_base.csv
            step_raw
                random.csv


    Also create the random files with cols
    id: random it
    datetime: random datetime
    text: random text
    tags: random tags
    """
    import datetime
    import os
    import random
    import shutil
    import string

    import pandas as pd

    if os.path.exists("./data"):
        shutil.rmtree("./data")
    os.mkdir("./data")
    os.mkdir("./data/fr")
    os.mkdir("./data/fr/step_raw")
    os.mkdir("./data/fr/step_raw/v_1")
    os.mkdir("./data/es")
    os.mkdir("./data/es/step_raw")

    def random_string(length=10):
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    def random_datetime():
        start = datetime.datetime.strptime("1/1/2008 1:30 PM", "%m/%d/%Y %I:%M %p")
        end = datetime.datetime.strptime("1/1/2009 4:00 PM", "%m/%d/%Y %I:%M %p")

        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + datetime.timedelta(seconds=random_second)

    def random_tags():
        return random_string(5) + "," + random_string(5) + "," + random_string(5)

    df_fr = pd.DataFrame(
        {
            "id": [random.randint(0, 100) for _ in range(100)],
            "datetime": [random_datetime() for _ in range(100)],
            "text": [random_string(10) for _ in range(100)],
            "tags": [random_tags() for _ in range(100)],
        }
    )
    df_fr.to_csv("./data/fr/step_raw/v_1/random.csv", index=False)

    df_es = pd.DataFrame(
        {
            "id": [random.randint(0, 100) for _ in range(100)],
            "datetime": [random_datetime() for _ in range(100)],
            "text": [random_string(10) for _ in range(100)],
            "tags_es": [random_tags() for _ in range(100)],
            "tags_en": [random_tags() for _ in range(100)],
        }
    )
    df_es.to_csv("./data/es/step_raw/random.csv", index=False)
    df_es.loc[:, ["id", "text"]].to_csv("./data/es/random_base.csv", index=False)
