#!/usr/bin/env python3
import shlex
import subprocess
import multiprocessing

import click
from unipath import Path


PROCESSES = 50


def yolo_detect_image_task(image_file, temp_dir, darknet_dir):
    filename = image_file.name.split('/')[-1]
    clean_filename = filename.split('.')[0]

    pred_file = Path(darknet_dir, 'pred-{}.png'.format(clean_filename))
    command = ' '.join([
        darknet_dir.child('darknet'),
        'detect',
        darknet_dir.child('cfg', 'yolov3.cfg'),
        darknet_dir.child('yolov3.weights'),
        '"{}"'.format(image_file.absolute()),
        '-out',
        '"{}"'.format(pred_file.name.split('.')[0]),
    ])
    print('Exec --> {}'.format(command))

    detect = subprocess.Popen(
        shlex.split(command),
        #stdout=subprocess.DEVNULL,
        #stderr=subprocess.DEVNULL,
        cwd=darknet_dir,
    )
    detect.wait()


@click.command()
@click.argument('darknet_dir', type=click.Path(exists=True))
@click.argument('images_dir', type=click.Path(exists=True))
@click.option('--temp_dir', default='/tmp/')
def cli(darknet_dir, images_dir, temp_dir):
    darknet_dir = Path(darknet_dir)
    images_dir = Path(images_dir)
    temp_dir = Path(temp_dir)

    if not temp_dir.exists():
        print('Dir "{}" does not exist'.format(temp_dir))
        return

    with multiprocessing.Pool(PROCESSES) as pool:
        args = []
        for image_file in images_dir.listdir():
            args.append((image_file, temp_dir, darknet_dir))

        pool.starmap(yolo_detect_image_task, args)


if __name__ == '__main__':
    cli()
