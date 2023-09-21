import pickle


# TODO move this in utils
def save_to_pkl(obj, filename):
    with open(filename, "wb") as f:
        pickle.dump(obj, f)


def load_from_pkl(filename):
    with open(filename, "rb") as f:
        obj = pickle.load(f)
    return obj

