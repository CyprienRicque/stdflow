import os


def export_env_var():
    to_export = os.environ["stdflow_hello"]

    with open("resultfn.txt", "w") as f:
        f.write(to_export)
    print(f"exported stdflow_hello {to_export}")
