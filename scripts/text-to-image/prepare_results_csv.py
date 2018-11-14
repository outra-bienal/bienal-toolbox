#!/usr/bin/env python3
import click
import rows
from PIL import Image
import json
from csv import DictWriter
from tqdm import tqdm


class Transcriptions():

    def __init__(self, transcription_file):
        self.curr_word_index = 0
        with open(transcription_file, 'r') as fd:
            data = json.load(fd)
            self.transcriptions = data['words']

    @property
    def curr_word(self):
        return self.transcriptions[self.curr_word_index]

    @property
    def next_word(self):
        return self.transcriptions[self.curr_word_index + 1]

    def get_duration(self, index, word):
        curr_word = self.curr_word

        try:
            clean_word = curr_word['word'].strip().lower()

            if word == clean_word:
                self.curr_word_index += 1
                return curr_word['start'], curr_word['end']
            elif '-' in word:
                next_word = self.next_word
                if word == '{}-{}'.format(clean_word, self.next_word['word'].strip().lower()):
                    self.curr_word_index += 2
                    return curr_word['start'], next_word['end']

            return None, None
        except KeyError:
            return None, None


@click.command()
@click.argument("results_csv", type=click.Path(exists=True))
@click.argument("transcription_file", type=click.Path(exists=True))
def prepare_results_csv(results_csv, transcription_file):
    trans = Transcriptions(transcription_file)

    duration_rows = []
    images = rows.import_from_csv(results_csv)
    for i, img_data in tqdm(enumerate(images)):
        img = Image.open(img_data.image_file)
        width, height = img.size
        word = img_data.search.strip().lower()

        start, end = trans.get_duration(i, word)

        data = img_data._asdict()
        data['start'] = start
        data['end'] = end
        duration_rows.append(data)

    duration_csv = results_csv.split('.')[0] + '-durations.csv'
    fieldnames = images.field_names + ['start', 'end', 'duration']
    with open(duration_csv, 'w') as fd:
        writer = DictWriter(fd, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(duration_rows)


if __name__ == '__main__':
    prepare_results_csv()
