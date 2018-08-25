#!/usr/bin/env python
import click
import os
import shlex
import subprocess
import tqdm
from multiprocessing import Pool
from unipath import Path


# pip3 install click unipath tqdm


NUM_OF_PROCESSES = 8


def convert_to_png(pdf, out):
    command = ' '.join([
        'pdftoppm',
        '-rx', '300',
        '-ry', '300',
        '"{}"'.format(pdf),
        '"{}"'.format(out),
        '-png'
    ])

    convert = subprocess.Popen(
        shlex.split(command),
    )
    convert.wait()


@click.command()
@click.argument("pdfs_dir", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path(exists=False))
def convert_pdfs(pdfs_dir, output_dir):
    output_dir = Path(output_dir)
    if not output_dir.exists():
        os.makedirs(output_dir)

    pdfs_dir = Path(pdfs_dir)
    args = []
    for pdf in tqdm.tqdm(pdfs_dir.listdir("*.pdf")):
        f_name = pdf.name.split('.')[0]
        out = output_dir.child(f_name)
        args.append((pdf, out))

    with Pool(NUM_OF_PROCESSES) as pool:
        pool.starmap(convert_to_png, args)

#    for img in output_dir.listdir('*.png'):
#        name = img.name.replace('-1', '')
#        img.rename(output_dir.child(name))


if __name__ == '__main__':
    convert_pdfs()
