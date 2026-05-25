import os
import subprocess
import sys

def load_env():
    env_file = ".env.local"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    os.environ[k.strip()] = v.strip()

if __name__ == "__main__":
    load_env()
    os.environ["PORT"] = "7860"
    
    # Run the app.py using the current python interpreter
    # which will be the one from the virtual environment
    subprocess.run([sys.executable, "app.py"])
