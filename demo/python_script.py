

import os


to_export = os.environ['stdflow_hello']

with open('result_scr.txt', 'w') as f:
    f.write(to_export)
print(f"exported stdflow_hello {to_export}")

