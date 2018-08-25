#!/usr/bin/env python
import click
import os
import shlex
import subprocess
from multiprocessing import Pool
from unipath import Path

from frames_ranges import *


NUM_OF_PROCESSES = 8
S3_BUCKET = 's3://videos-frame-images/'
FRAMES_BY_SERVICE = {
    'detectron': (DETECTRON_LEVA_1, DETECTRON_LEVA_2),
    'yolo': (YOLO_LEVA_1, YOLO_LEVA_2),
    'detectron-extra': ([], DETECTRON_EXTRA_LEVA_2),
}


def get_frames_from_range(start, end):
    prefix = start.split(' ')[0]
    start_i = int(start.split(' ')[1].split('-')[0])
    end_i = int(end.split(' ')[1].split('-')[0])
    for frame in range(start_i, end_i + 1):
        yield "{} {}".format(prefix, frame)


def frames_iter(frames_ranges, s3_dir, ext=''):
    if ext:
        ext = '.{}'.format(ext)
    for start, end in frames_ranges:
        for frame in get_frames_from_range(start, end):
            yield '{}{}{}.jpg{}'.format(
                S3_BUCKET, s3_dir, frame, ext
            )


def get_detectron_pdfs_frames_paths(leva_1_ranges, leva_2_ranges):
    dir_1, dir_2 = 'detectron-results/leva-1/', 'detectron-results/leva-2/'
    for frame in frames_iter(leva_1_ranges, dir_1, ext='pdf'):
        yield frame
    for frame in frames_iter(leva_2_ranges, dir_2, ext='pdf'):
        yield frame


def get_video_frames_path(leva_1_ranges, leva_2_ranges):
    dir_1, dir_2 = 'yolo_leva_1/', 'yolo_leva_2/'
    for frame in frames_iter(leva_1_ranges, dir_1):
        yield frame
    for frame in frames_iter(leva_2_ranges, dir_2):
        yield frame


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
@click.argument("service_frames", type=str)
@click.argument("output_dir", type=click.Path(exists=False))
@click.option('--pdfs-only/--frames-only', default=False)
def download_frames(service_frames, output_dir, pdfs_only):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.makedirs(output_dir)

    args = []
    if service_frames in FRAMES_BY_SERVICE:
        frames_ranges = FRAMES_BY_SERVICE[service_frames]
    else:
        print('Service "{}" does not have frames ranges. Options are:'.format(service_frames))
        for k in FRAMES_BY_SERVICE:
            print('\t- {}'.format(k))
        return

    if pdfs_only:
        for pdf in get_detectron_pdfs_frames_paths(*frames_ranges):
            args.append((pdf, output_dir))
    else:
        for frame in get_video_frames_path(*frames_ranges):
            args.append((frame, output_dir))

    print('Donwloading {} frames...'.format(len(args)))
    with Pool(NUM_OF_PROCESSES) as pool:
        pool.starmap(download_s3_file, args)


if __name__ == '__main__':
    download_frames()
