#!/usr/bin/env python3
import click
import cv2
import os

from tqdm import tqdm
from bienal import BienalClient
from unipath import Path
import requests

TEMP_DIR = Path('/', 'tmp')


def extract_faces(image_data, out_dir):
    faces = image_data.ibm.faces.get('faces', [])
    if not faces:
        return

    image_response = requests.get(image_data['image'])
    if not image_response.ok:
        print("Error downloading image")
        return

    filename = image_data['image'].split('/')[-1]
    splited = filename.split('.')
    filename = '.'.join(splited)
    temp_file = TEMP_DIR.child(filename)

    with open(temp_file, 'bw') as fd:
        fd.write(image_response.content)

    img = cv2.imread(temp_file)
    for i, face in enumerate(faces):
        loc = face['face_location']
        x, y = loc['left'], loc['top']
        w, h = loc['width'], loc['height']

        offset = 15
        x -= offset
        y -= offset
        w += offset
        h += offset

        crop_face = img[y:y+h, x:x+w]
        head_name = out_dir.child("head-{}-{}".format(i, filename))
        cv2.imwrite(head_name, crop_face)


@click.command()
@click.argument("output_dir", type=click.Path(exists=False))
def get_faces(output_dir):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.makedirs(output_dir)

    bienal = BienalClient()
    for col in bienal.get_all_collections():
        print('Processando imagens da coleção "{}"'.format(col['title']))
        for image in tqdm(col.images):
            extract_faces(image, output_dir)
        print()


if __name__ == '__main__':
    get_faces()
