#!/usr/bin/env python3
import click
import rows
from tqdm import tqdm
from unipath import Path
from shutil import copyfile
import os


@click.command()
@click.argument("results_csv", type=click.Path(exists=True))
@click.option("--output-dir", default='frames', help='Frames output dir')
@click.option("--frame-rate", default=24, help='Frame Rate')
def generate_frames(results_csv, output_dir, frame_rate):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.mkdir(output_dir)

    images = rows.import_from_csv(results_csv)

    frames_count = 0
    extra = 0
    last_end = 0
    for i, img_data in tqdm(enumerate(images)):
        duration = img_data.end - img_data.start + img_data.start - last_end
        if duration < 0.2:
            extra += duration
            continue

        frames_part = duration * frame_rate + extra
        frames = int(frames_part)
        original_img = Path(img_data.image_file)

        for i in range(frames):
            frames_count += 1

            num_len = len(str(frames_count))
            zeros = '0' * (6 - num_len)
            frame_path = output_dir.child("frame_{}{}.jpg".format(zeros, frames_count))

            copyfile(original_img, frame_path)

        extra = frames_part - frames
        last_end = img_data.end


if __name__ == '__main__':
    generate_frames()
