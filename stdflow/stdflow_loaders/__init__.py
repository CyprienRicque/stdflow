from stdflow.stdflow_loaders.csv import convert_data, load_csv_files


class DataLoader:
    def __init__(self, dir_paths, loader=None):
        self.dit_path = dir_paths
        self.loader = loader or load_csv_files

    def typed_load_data(self, type_="dict"):
        data = self.loader(self.dit_path)
        return convert_data(data, type_)
