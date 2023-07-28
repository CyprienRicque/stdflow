import pandas as pd
import pytest
import os

# Test setup function to create an Excel file
@pytest.fixture(scope="module")
def create_excel_file(tmpdir_factory):
    # Temporary directory
    fn = tmpdir_factory.mktemp("data").join("test.xlsx")

    # Sample data
    data = {'Name': ['Tom', 'Nick', 'John', 'Tom', 'John'],
            'Age': [20, 21, 19, 20, 18],
            'Score': [85, 87, 92, 86, 90]}

    # Creating multiple sheets
    with pd.ExcelWriter(fn) as writer:
        for i in range(3):
            data[f'Name{i}'] = [f'{name}{i+1}' for name in data['Name']]
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name=f'Sheet{i+1}', index=False)
            del data[f'Name{i}']

    return str(fn)


def test_load_excel_multiple_sheets(create_excel_file):
    # Load Excel with multiple sheets
    import stdflow as sf
    step = sf.Step()
    result = step.load(file_name=create_excel_file, sheet_name=None)

    # Check if the loaded data is a dictionary (multiple sheets will be loaded as dict)
    assert isinstance(result, dict)

    # Check if all sheets have been loaded
    assert len(result.keys()) == 3

    # Check if the data in the sheets are loaded correctly
    for sheet in result:
        i = int(sheet[-1]) - 1
        assert result[sheet].equals(pd.DataFrame({
            'Name': [f'Tom', f'Nick', f'John', f'Tom', f'John'],
            'Age': [20, 21, 19, 20, 18],
            'Score': [85, 87, 92, 86, 90],
            f'Name{i}': [f'Tom{i+1}', f'Nick{i+1}', f'John{i+1}', f'Tom{i+1}', f'John{i+1}']
        }))

    assert [c['name'] for c in step.data_l[0].columns] == ['Name', 'Age', 'Score', 'Name0', "Name", "Age", "Score", "Name1", "Name", "Age", "Score", "Name2"]

    # Remove the file after the test
    os.remove(create_excel_file)
