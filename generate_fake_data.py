import os
from faker import Faker


def generate_text_file_in_MB(size):
    fake = Faker()
    FINAL_FILESIZE = size * 1000000
    FILENAME = f'data/words{size}MB.txt'
    filesize = 0
    i = 0
    while filesize < FINAL_FILESIZE:
        with open(FILENAME, 'a') as f:
            text = fake.text()
            f.write(text)
        filesize = os.path.getsize(FILENAME)
        i += 1
        print(f"This is loop number {i}")
        print(f"{FILENAME} current file size is {filesize}")

filesizes = [1,5,10,20,100]

for fsize in filesizes:
    generate_text_file_in_MB(fsize)
