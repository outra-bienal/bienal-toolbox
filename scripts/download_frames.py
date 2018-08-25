#!/usr/bin/env python
import click
import os
import shlex
import subprocess
from multiprocessing import Pool
from unipath import Path


NUM_OF_PROCESSES = 8
S3_BUCKET = 's3://videos-frame-images/'


leva_1_ranges = [
    ("C0001 200010-1", "C0001 200165-1"),
    ("C0001 201678-1", "C0001 202134-1"),
    ("C0001 202582-1", "C0001 202979-1"),
    ("C0001 203098-1", "C0001 203487-1"),
    ("C0001 203768-1", "C0001 204275-1"),
    ("C0001 204838-1", "C0001 205083-1"),
    ("C0001 205873-1", "C0001 205936-1"),
    ("C0001 205986-1", "C0001 206091-1"),
    ("C0001 206234-1", "C0001 207347-1"),
    ("C0001 208620-1", "C0001 208912-1"),
    ("C0001 210595-1", "C0001 210866-1"),
    ("C0001 209947-1", "C0001 210245-1")
]

leva_2_ranges = [
    ("C0011 200023-1", "C0011 200418-1"),
    ("C0011 201020-1", "C0011 201149-1"),
    ("C0011 201797-1", "C0011 202203-1"),
    ("C0011 203995-1", "C0011 204037-1"),
    ("C0011 204091-1", "C0011 204193-1"),
    ("C0011 204408-1", "C0011 204433-1"),
    ("C0011 204500-1", "C0011 204559-1"),
    ("C0011 205181-1", "C0011 205219-1"),
    ("C0011 205223-1", "C0011 205240-1"),
    ("C0011 205349-1", "C0011 205386-1"),
    ("C0011 206857-1", "C0011 206864-1"),
    ("C0011 206938-1", "C0011 206947-1"),
    ("C0011 207062-1", "C0011 207239-1"),
    ("C0011 207453-1", "C0011 207745-1"),
    ("C0011 207746-1", "C0011 207809-1"),
    ("C0011 207850-1", "C0011 208138-1"),
    ("C0011 208289-1", "C0011 208417-1"),
    ("C0011 208418-1", "C0011 209205-1"),
    ("C0011 209882-1", "C0011 210337-1"),
    ("C0011 210854-1", "C0011 211279-1"),
    ("C0011 211478-1", "C0011 211671-1"),
    ("C0011 213284-1", "C0011 213455-1"),
    ("C0011 213456-1", "C0011 214267-1"),
    ("C0011 215153-1", "C0011 215427-1"),
    ("C0011 215912-1", "C0011 216093-1"),
    ("C0011 218119-1", "C0011 218135-1"),
    ("C0011 218159-1", "C0011 218175-1"),
    ("C0011 218217-1", "C0011 218223-1"),
    ("C0011 218240-1", "C0011 218252-1"),
    ("C0011 218269-1", "C0011 218272-1"),
    ("C0011 218285-1", "C0011 218288-1"),
    ("C0011 218443-1", "C0011 218502-1"),
    ("C0011 218516-1", "C0011 218535-1"),
    ("C0011 218755-1", "C0011 218774-1"),
    ("C0011 218884-1", "C0011 218896-1"),
    ("C0011 218915-1", "C0011 218918-1"),
    ("C0011 218928-1", "C0011 218963-1"),
    ("C0011 221668-1", "C0011 221910-1"),
    ("C0011 219994-1", "C0011 220265-1"),
    ("C0011 221961-1", "C0011 222001-1"),
    ("C0011 222119-1", "C0011 222936-1"),
    ("C0011 224713-1", "C0011 224780-1"),
    ("C0011 225379-1", "C0011 225861-1"),
    ("C0011 226463-1", "C0011 226710-1"),
    ("C0011 226715-1", "C0011 226720-1"),
    ("C0011 226726-1", "C0011 226736-1"),
    ("C0011 226739-1", "C0011 226740-1"),
    ("C0011 226743-1", "C0011 226748-1"),
    ("C0011 226835-1", "C0011 226988-1"),
    ("C0011 226989-1", "C0011 227674-1"),
    ("C0011 227973-1", "C0011 228279-1"),
    ("C0011 228851-1", "C0011 228853-1"),
    ("C0011 228861-1", "C0011 228865-1"),
    ("C0011 228916-1", "C0011 228926-1"),
    ("C0011 228927-1", "C0011 228942-1"),
    ("C0011 229070-1", "C0011 229073-1"),
    ("C0011 229127-1", "C0011 229156-1"),
    ("C0011 229319-1", "C0011 231168-1"),
    ("C0011 231191-1", "C0011 231373-1"),
    ("C0011 231506-1", "C0011 231647-1"),
    ("C0011 232704-1", "C0011 232752-1"),
    ("C0011 233161-1", "C0011 233166-1"),
    ("C0011 233173-1", "C0011 233187-1"),
    ("C0011 233191-1", "C0011 233193-1"),
    ("C0011 233293-1", "C0011 233319-1"),
    ("C0011 233987-1", "C0011 234105-1"),
    ("C0011 236925-1", "C0011 237220-1"),
    ("C0011 238514-1", "C0011 238545-1"),
    ("C0011 238579-1", "C0011 238700-1"),
    ("C0011 238914-1", "C0011 239006-1"),
    ("C0011 239309-1", "C0011 239366-1"),
    ("C0011 240733-1", "C0011 240848-1"),
    ("C0011 241642-1", "C0011 241929-1"),
    ("C0011 241968-1", "C0011 242247-1"),
    ("C0011 242575-1", "C0011 242800-1"),
    ("C0011 242988-1", "C0011 243239-1"),
]


def get_frames_from_range(start, end):
    prefix = start.split(' ')[0]
    start_i = int(start.split(' ')[1].split('-')[0])
    end_i = int(end.split(' ')[1].split('-')[0])
    for frame in range(start_i, end_i + 1):
        yield "{} {}".format(prefix, frame)


def get_detectron_pdfs_frames_paths():
    dir_1, dir_2 = 'detectron-results/leva-1/', 'detectron-results/leva-2/'
    for start, end in leva_1_ranges:
        for frame in get_frames_from_range(start, end):
            yield '{}{}{}.jpg.pdf'.format(
                S3_BUCKET, dir_1, frame
            )

    for start, end in leva_2_ranges:
        for frame in get_frames_from_range(start, end):
            yield '{}{}{}.jpg.pdf'.format(
                S3_BUCKET, dir_2, frame
            )


def get_video_frames_path():
    dir_1, dir_2 = 'yolo_leva_1/', 'yolo_leva_2/'
    for start, end in leva_1_ranges:
        for frame in get_frames_from_range(start, end):
            yield '{}{}{}.jpg'.format(
                S3_BUCKET, dir_1, frame
            )

    for start, end in leva_2_ranges:
        for frame in get_frames_from_range(start, end):
            yield '{}{}{}.jpg'.format(
                S3_BUCKET, dir_2, frame
            )


def download_s3_file(filepath, output_dir):
    command = ' '.join([
        'aws',
        's3',
        'cp',
        '"{}"'.format(filepath),
        '"{}"'.format(output_dir),
    ])
    download = subprocess.Popen(shlex.split(command))
    download.wait()


@click.command()
@click.argument("output_dir", type=click.Path(exists=False))
@click.option("--detectron_pdfs", default=False)
def download_frames(output_dir, detectron_pdfs):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.makedirs(output_dir)

    args = []

    if detectron_pdfs:
        for pdf in get_detectron_pdfs_frames_paths():
            args.append(pdf, output_dir)
    else:
        for frame in get_video_frames_path():
            args.append(frame, output_dir)

    print('Donwloading {} frames...'.format(len(args)))
    with Pool(NUM_OF_PROCESSES) as pool:
        pool.starmap(download_s3_file, args)


if __name__ == '__main__':
    download_frames()
