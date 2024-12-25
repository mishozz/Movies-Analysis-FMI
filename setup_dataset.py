import kagglehub
import shutil
import os

OLD_NEW_NAME_MAP = {
    "title.ratings.tsv": "ratings.tsv",
    "name.basics.tsv.gz": "actors.tsv",
    "title.basics.tsv": "titles.tsv"
}

current_directory = os.getcwd()

path = kagglehub.dataset_download("ashirwadsangwan/imdb-dataset")
print("Path to dataset files:", path)

# Copy files to the current directory
for filename in os.listdir(path):
    full_file_name = os.path.join(path, filename)
    if os.path.isfile(full_file_name):
        shutil.copy(full_file_name, os.getcwd())

print("Files copied to the current directory.")

for old_name, new_name in OLD_NEW_NAME_MAP.items():
    old_file = os.path.join(current_directory, old_name)
    new_file = os.path.join(current_directory, new_name)
    if os.path.isfile(old_file):
        os.rename(old_file, new_file)

print("Dataset is fully initialised.")
