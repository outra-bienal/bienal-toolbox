#!/usr/bin/env python3
import click
import rows
from tqdm import tqdm
from unipath import Path
from shutil import copyfile
import os
from PIL import Image
from decimal import Decimal, getcontext


getcontext().prec = 4


@click.command()
@click.argument("results_csv", type=click.Path(exists=True))
@click.option("--output-dir", default='frames', help='Frames output dir')
@click.option("--frame-rate", default=24, help='Frame Rate')
@click.option("--video-width", default=1200, help='Video width')
def generate_frames(results_csv, output_dir, frame_rate, video_width):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.mkdir(output_dir)

    images = rows.import_from_csv(results_csv)

    frames_count = 0
    extra = 0
    last_end = 0

    for i, img_data in tqdm(enumerate(images)):
        if img_data.start is None or img_data.end is None:
            continue

        end = Decimal(img_data.end)

        duration = end - last_end
        if duration < 0.2:
            extra += duration
            last_end = end
            continue

        frames_part = (duration + extra) * frame_rate
        frames = int(frames_part)
        original_img = Path(img_data.image_file)

        width, height = Image.open(original_img).size
        scale = width >= video_width or height >= video_width

        for i in range(frames):
            frames_count += 1

            num_len = len(str(frames_count))
            zeros = '0' * (6 - num_len)
            frame_path = output_dir.child("frame_{}{}.jpg".format(zeros, frames_count))

            copyfile(original_img, frame_path)
            if scale:
                size = width, height
                while size[0] > video_width or size[1] > video_width:
                    size = size[0] * 0.5, size[1] * 0.5

                frame_img = Image.open(frame_path)
                frame_img.thumbnail(size, Image.ANTIALIAS)
                frame_img.save(frame_path, "JPEG")

        extra = (frames_part - frames) / frame_rate
        last_end = end


if __name__ == '__main__':
    generate_frames()
