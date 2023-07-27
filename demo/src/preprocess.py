import stdflow as sf


# def format_column_names():
#     """
#     Format column names to be more readable
#     replace underscores with spaces and capitalize each word
#     """
step = sf.Step()
step.root = "./demo_data"
step.file_name = 'countries of the world.csv'

df = step.load(attrs=['countries'], step='load')

df.columns = df.columns.str.replace('_', ' ').str.title()

step.save(df, attrs=['countries'], step="col_renamed")
# return df


# if __name__ == '__main__':
#     format_column_names()

