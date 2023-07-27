import os

with open('/tmp/env_script.txt', 'w') as f:
    for key, value in os.environ.items():
        if key.startswith('__stdflow__'):
            f.write(f'{key}={value}\n')
