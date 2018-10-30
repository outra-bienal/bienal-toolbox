#!/usr/bin/env python3
from tqdm import tqdm
import click
import requests


class ArtDecoderApiClient():

    host = "https://art-decoder.bienal.berinfontes.com"

    def get_all_collections(self):
        url = self.host + '/api/collection'
        response = requests.get(url)
        if response.ok:
            return response.json()

    def get_collection(self, col_id):
        path = '/api/collection/{}'.format(col_id)
        url = self.host + path
        response = requests.get(url)
        if response.ok:
            return response.json()


class DenseCaptions():

    def __init__(self):
        self.captions = set()

    def add(self, caption):
        caption = caption.lower().strip()
        self.captions.add(caption)

    def all(self):
        return iter(self.captions)


def extract_captions_from_image(image):
    all_captions = []

    if image['deepAi']:
        captions = image["deepAi"]["DenseCap"]["output"]["captions"]
        all_captions.extend([c['caption'] for c in captions])
    elif image['microsoftazure']:
        captions = image["microsoftazure"]['main']["description"]["captions"]
        all_captions.extend([c['text'] for c in captions])

    return all_captions


@click.command()
@click.argument("output_file", type=click.Path(exists=False))
def get_all_dense_captions(output_file):
    client = ArtDecoderApiClient()
    collections = [{'id': 15}]

    dense_captions = DenseCaptions()
    print('Getting captions for all collections')
    for c_id in tqdm([c['id'] for c in collections]):
        collection = client.get_collection(c_id)

        for image in collection['images']:
            for caption in extract_captions_from_image(image):
                dense_captions.add(caption)

    with open(output_file, 'w') as fd:
        fd.writelines([l + '\n' for l in sorted(dense_captions.all())])


if __name__ == '__main__':
    get_all_dense_captions()
