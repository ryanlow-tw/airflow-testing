import os
from faker import Faker

def main():
    chars_in_sentence = 100

    num_sentences = [10000,20000,50000,100000,200000,500000,1000000,2000000,5000000,10000000]

    total_chars_in_sentence = list(
        map(
            lambda sentences: sentences * chars_in_sentence, num_sentences))

    faker = Faker()
    for sentence in total_chars_in_sentence:
        filename = f'data/words_{sentence // 100}sentence.txt'
        with open(filename, 'a') as f:
            f.write(faker.text(sentence))
        filesize = os.path.getsize(f'data/words_{sentence // 100}sentence.txt')
        print(f'num sentence: {sentence}, filesize: {filesize}')


if __name__ == "__main__":
    main()
    