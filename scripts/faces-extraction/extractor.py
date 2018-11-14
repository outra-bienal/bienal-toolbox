#!/usr/bin/env python3
import click
import cv2
import os
from random import choice
from PIL import Image

from tqdm import tqdm
from bienal import BienalClient
from unipath import Path
import requests

TEMP_DIR = Path('/', 'tmp')


def extract_faces(image_data, out_dir):
    #faces = image_data.ibm.faces.get('faces', [])
    faces = image_data.aws.faces.get('FaceDetails', [])
    if not faces:
        return

    filename = image_data['image'].split('/')[-1]
    splited = filename.split('.')
    temp_file = TEMP_DIR.child(filename)

    if not temp_file.exists():
        image_response = requests.get(image_data['image'])
        if not image_response.ok:
            print("Error downloading image")
            return

        with open(temp_file, 'bw') as fd:
            fd.write(image_response.content)

    img = cv2.imread(temp_file)
    height, width = img.shape[:2]
    offset = 15

    for i, face in enumerate(faces):
        loc = face['BoundingBox']
        x, y = loc['Left'], loc['Top']
        w, h = loc['Width'], loc['Height']
        x = int(x * width - offset)
        y = int(y * height - offset)
        w = int(w * width + offset)
        h = int(h * height + offset)
        if x < 0:
            x = 0
        if y < 0:
            y = 0
        if w > width:
            w = width
        if h > height:
            h = height

        crop_face = img[y:y+h, x:x+w]
        head_name = out_dir.child("head-{}-{}.{}".format(splited[0], i, splited[1]))
        if head_name.exists():
            random_i = choice(range(1000000))
            head_name = out_dir.child("head-{}-{}.{}".format(splited[0], random_i, splited[1]))

        cv2.imwrite(head_name, crop_face)


def replace_faces(image_data, out_dir):
    faces = image_data.aws.faces.get('FaceDetails', [])
    if not faces:
        return

    filename = image_data['image'].split('/')[-1]
    temp_file = TEMP_DIR.child(filename)

    if not temp_file.exists():
        image_response = requests.get(image_data['image'])
        if not image_response.ok:
            print("Error downloading image")
            return

        with open(temp_file, 'bw') as fd:
            fd.write(image_response.content)

    img = Image.open(temp_file)
    pixels = img.load()
    height, width = img.height, img.width
    offset = 15

    dominant_colors = image_data.google.image_properties_annotation['dominantColors']['colors']
    colors = [(c['red'], c['green'], c['blue']) for c in [d['color'] for d in dominant_colors]]

    for i, face in enumerate(faces):
        loc = face['BoundingBox']
        x, y = loc['Left'], loc['Top']
        w, h = loc['Width'], loc['Height']
        x = int(x * width - offset)
        y = int(y * height - offset)
        w = int(w * width + offset)
        h = int(h * height + offset)
        if x < 0:
            x = 0
        if y < 0:
            y = 0
        if w > width:
            w = width
        if h > height:
            h = height

        for current_x in range(x, x + w):
            for current_y in range(y, y + h):
                pixels[current_x, current_y] = choice(colors)

    output = out_dir.child("replace-{}".format(filename))
    img.save(output)


@click.command()
@click.argument("action_type")
@click.argument("output_dir", type=click.Path(exists=False))
def get_faces(action_type, output_dir):
    if action_type not in ['extract', 'replace']:
        print("Opção {} é inválida".format(action_type))

    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.makedirs(output_dir)

    bienal = BienalClient()
    for col in bienal.get_all_collections():
        print('Processando imagens da coleção "{}"'.format(col['title']))
        for image in tqdm(col.images):
            if action_type == 'extract':
                extract_faces(image, output_dir)
            elif action_type == 'replace':
                replace_faces(image, output_dir)

        print()


if __name__ == '__main__':
    get_faces()
