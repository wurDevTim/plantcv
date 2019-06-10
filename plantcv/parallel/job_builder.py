from __future__ import print_function
import os
import sys
import re


# Build job list
###########################################
def job_builder(meta, valid_meta, workflow, job_dir, out_dir, coprocess=None, other_args="", writeimg=False):
    """Build a list of image processing jobs.

    Args:
        meta:         Dictionary of processed image metadata.
        valid_meta:   Dictionary of valid metadata keys.
        workflow:     PlantCV image processing workflow script file.
        job_dir:      Intermediate file output directory.
        out_dir:      Output images directory.
        coprocess:    Coprocess the specified imgtype with the imgtype specified in meta_filters.
        other_args:   String of additional arguments to be passed to the workflow script.
        writeimg:     Boolean that specifies whether output images should be created or not.

    Returns:
        jobs:         List of image processing commands.

    :param meta: dict
    :param valid_meta: dict
    :param workflow: str
    :param job_dir: str
    :param out_dir: str
    :param coprocess: str
    :param other_args: str
    :param writeimg: bool
    :return job_stack: list
    """

    # Overall job stack. List of list of jobs
    jobs = []

    # Get the list of images
    # images = list(meta.keys())
    images = []
    for img in list(meta.keys()):
        # # If a date range was requested, check whether the image is within range
        # if args.dates:
        #     # Convert image datetime to unix time
        #     timestamp = dt_parser(meta[img]['timestamp'])
        #     time_delta = timestamp - datetime.datetime(1970, 1, 1)
        #     unix_time = (time_delta.days * 24 * 3600) + time_delta.seconds
        #     if unix_time < args.start_date or unix_time > args.end_date:
        #         continue
        if coprocess is not None:
            if meta[img]['imgtype'] != coprocess:
                images.append(img)
        else:
            images.append(img)

    print("Job list will include " + str(len(images)) + " images" + '\n', file=sys.stderr)

    # For each image
    for img in images:
        if (coprocess is not None) and ('coimg' in meta[img]):
            # Create an output file to store the co-image processing results and populate with metadata
            coimg = meta[meta[img]['coimg']]
            coout = open(os.path.join(".", job_dir, meta[img]["coimg"] + ".txt"), 'w')
            coout.write('\t'.join(map(str, ("META", "image", os.path.join(coimg['path'], meta[img]['coimg'])))) + '\n')
            # Valid metadata
            for m in list(valid_meta.keys()):
                coout.write('\t'.join(map(str, ("META", m, coimg[m]))) + '\n')

        # Create an output file to store the image processing results and populate with metadata
        outfile = open(os.path.join(".", job_dir, img + ".txt"), 'w')
        outfile.write('\t'.join(map(str, ("META", "image", os.path.join(meta[img]['path'], img)))) + '\n')
        # Valid metadata
        for m in list(valid_meta.keys()):
            outfile.write('\t'.join(map(str, ("META", m, meta[img][m]))) + '\n')

        outfile.close()

        # Build job
        job_parts = ["python", workflow, "--image", os.path.join(meta[img]['path'], img),
                     "--outdir", out_dir, "--result", os.path.join(job_dir, img) + ".txt"]
        # Add job to list
        if coprocess is not None and ('coimg' in meta[img]):
            job_parts = job_parts + ["--coresult", os.path.join(job_dir, meta[img]['coimg']) + ".txt"]
        if writeimg:
            job_parts.append("--writeimg")
        if other_args:
            other_args1 = re.sub("'", "", other_args)
            other_args = other_args1.split(" ")
            job_parts = job_parts + other_args
        jobs.append(job_parts)

    return jobs
###########################################
