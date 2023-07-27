import os

def export_env_var():
    with open('/tmp/env_function.txt', 'w') as f:
        for key, value in os.environ.items():
            if key.startswith('__stdflow__'):
                f.write(f'{key}={value}\n')

