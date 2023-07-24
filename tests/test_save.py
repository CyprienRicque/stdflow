import os

import pandas as pd
import stdflow as sf


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
    import os
    import shutil
    import random
    import string
    import datetime
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
    df_es.loc[:, ['id', 'text']].to_csv("./data/es/random_base.csv", index=False)


setup()


def test_export():

    step = sf.Step()  # only necessary when doing custom pipeline, otherwise functions are accessible at package level

    df = step.load("./data", path="fr", step="raw", version="1", file_name="random.csv")
    assert df.shape == (100, 4)

    df["new_col"] = "new_col"
    step.save(df, "./data", path="fr", step="with_new_col", version="1", file_name="random.csv")


# def test_load():
#
#     step = sf.Step()  # only necessary when doing custom pipeline, otherwise functions are accessible at package level
#
#     df = step.load("./data", path="fr", step="raw", version="1", file_name="random.csv")
#     assert df.shape == (100, 4)
#
#     df = step.load("./data", path="fr", step="raw", version="last", file_name="random.csv")
#     assert df.shape == (100, 4)
#
#     df = step.load("./data", path="fr", step="raw", method=pd.read_csv, file_name="random.csv")
#     assert df.shape == (100, 4)
#
#

#
def test_save_merge():
    step = sf.Step()  # only necessary when doing custom pipeline, otherwise functions are accessible at package level

    df1 = step.load("./data", path="es", step="raw", version=None, file_name="random.csv")
    assert df1.shape == (100, 5)
    df2 = step.load("./data", path="es", file_name="random_base.csv")
    assert df2.shape == (100, 2)

    df_full = pd.merge(df1, df2, on='id', how='left')

    step.save(df_full, "./data", path="es", step="merge", file_name="merged.csv")
    assert os.path.exists("./data/es/step_merge/merged.csv")

    step.save(df_full, "./data", path="es", step="merge", version="v_202307241247", file_name="merged.csv")
    assert os.path.exists("./data/es/step_merge/v_202307241247/merged.csv")
    assert os.path.exists("./data/es/step_merge/merged.csv")
    assert os.path.exists("./data/es/step_merge/v_202307241247/metadata.json")
    assert os.path.exists("./data/es/step_merge/metadata.json")


def test_2_step():
    test_save_merge()

    step = sf.Step()

    df1 = step.load("./data", path="es", step="merge", version="v_202307241247", file_name="merged.csv")
    df2 = step.load("./data", path="fr", step="raw", version="1", file_name="random.csv")

    df_full = pd.merge(df1, df2, on='id', how='left')
    step.save(df_full, "./data", path="global", step="es_fr_merge", version="0", file_name="merged_g.csv")
    assert os.path.exists("./data/global/step_es_fr_merge/v_0/merged_g.csv")
    assert os.path.exists("./data/global/step_es_fr_merge/v_0/metadata.json")


if __name__ == "__main__":
    test_export()
    test_save_merge()
    test_2_step()

