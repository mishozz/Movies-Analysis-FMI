import kagglehub
import shutil
import os

current_directory = os.getcwd()

path = kagglehub.dataset_download("shivamb/netflix-shows")
print("Path to dataset files:", path)

# Copy files to the current directory
for filename in os.listdir(path):
    full_file_name = os.path.join(path, filename)
    if os.path.isfile(full_file_name):
        shutil.copy(full_file_name, os.getcwd())

print("Files copied to the current directory.")

files_to_rename = {
    "title.ratings.tsv": "ratings.tsv",
    "name.basics.tsv.gz": "actors.tsv",
    "title.basics.tsv": "titles.tsv"
}

for old_name, new_name in files_to_rename.items():
    old_file = os.path.join(current_directory, old_name)
    new_file = os.path.join(current_directory, new_name)
    if os.path.isfile(old_file):
        os.rename(old_file, new_file)

print("Dataset is fully initialised.")
print("You can now run the analysis script via the: python3 main.py")
print("You can install the required libraries by running: pip install -r requirements.txt")
