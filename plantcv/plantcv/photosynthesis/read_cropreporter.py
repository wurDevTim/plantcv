"""Read in fluorescence images from a .DAT file."""
import os
import numpy as np
import xarray as xr
from plantcv.plantcv import params
from plantcv.plantcv._debug import _debug
from plantcv.plantcv import PSII_data
from plantcv.plantcv import Spectral_data
from skimage.util import img_as_ubyte




def read_cropreporter(container_client, kwargs):
    """
    Read datacubes from PhenoVation B.V. CropReporter into a PSII_Data instance.

    Inputs:
        container_client = Azure Container client
        kwargs        = dictionary with cropreporter azure paths

    Returns:
        ps               = photosynthesis data in xarray DataArray format

    :param filename: str
    :return ps: plantcv.plantcv.classes.PSII_data
    """
    # Initialize metadata dictionary
    metadata_dict = {}

    # Parse .inf file and create dictionary with metadata stored within
    blob_client = container_client.get_blob_client(kwargs['hdr_blob_name'])
    stream = blob_client.download_blob(encoding='UTF-8')
    buffer = stream.readall().splitlines()
    for line in buffer:
        if "=" in line:
            key, value = line.rstrip("\n").split("=")
            metadata_dict[key] = value

    # Initialize PSII_data class
    ps = PSII_data()

    # INF file prefix and path
    #ps.filename = os.path.split(kwargs['hdr_blob_name'])[-1]
    #ps.datapath = os.path.dirname(kwargs['hdr_blob_name'])

    # Dark-adapted measurements
    _process_psd_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    # Light-adapted measurements
    _process_psl_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    # Dark-adapted PAM measurements
    _process_pmd_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    # Light-adapted PAM Pmeasurements
    _process_pml_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    # Chlorophyll fluorescence data
    _process_chl_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    # Spectral measurements
    _process_spc_data(ps=ps, metadata=metadata_dict, container_client=container_client, kwargs=kwargs)

    return ps


def _process_psd_data(ps, metadata, container_client, kwargs):
    """
    Create an xarray DataArray for a PSD dataset.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    
    if 'psd_blob_name' in kwargs:
        img_cube, frame_labels, frame_nums = _read_dat_file(dataset="PSD",
                                                            container_client=container_client,
                                                            blob_path=kwargs['psd_blob_name'],
                                                            height=int(metadata["ImageRows"]),
                                                            width=int(metadata["ImageCols"]))
        # If not all frames are saved the order is fixed
        # Phenovation does not update the framenumbers in the references.
        # Default frames (when SaveAllFrames == 0)
        f0_frame = 1
        fm_frame = 2
        # If the method is labeled FvFm
        if 'FvFmFrameF0' in metadata and metadata["SaveAllFrames"] != "0":
            f0_frame = int(metadata["FvFmFrameF0"]) + 1
            fm_frame = int(metadata["FvFmFrameFm"]) + 1
        # If the method is labeled DkOjip
        if 'DkOjipFrameF0' in metadata and metadata["SaveAllFrames"] != "0":
            f0_frame = int(metadata["DkOjipFrameF0"]) + 1  # data cube includes Fdark at the beginning
            fm_frame = int(metadata["DkOjipFrameFm"]) + 1
        frame_labels[0] = 'Fdark'
        frame_labels[f0_frame] = 'F0'
        frame_labels[fm_frame] = 'Fm'
        psd = xr.DataArray(
            data=img_cube[..., None],
            dims=('x', 'y', 'frame_label', 'measurement'),
            coords={'frame_label': frame_labels,
                    'frame_num': ('frame_label', frame_nums),
                    'measurement': ['t0']},
            name='ojip_dark'
        )
        psd.attrs["long_name"] = "OJIP dark-adapted measurements"
        ps.add_data(psd)

        _debug(visual=ps.ojip_dark.squeeze('measurement', drop=True),
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_PSD-frames.png"),
               col='frame_label',
               col_wrap=int(np.ceil(ps.ojip_dark.frame_label.size / 4)))


def _process_psl_data(ps, metadata, container_client, kwargs):
    """
    Create an xarray DataArray for a PSL dataset.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    if 'psl_blob_name' in kwargs:
        img_cube, frame_labels, frame_nums = _read_dat_file(dataset="PSL",
                                                            container_client=container_client,
                                                            blob_path=kwargs['psl_blob_name'],
                                                            height=int(metadata["ImageRows"]),
                                                            width=int(metadata["ImageCols"]))
        # If not all frames are saved the order is fixed
        # Phenovation does not update the framenumbers in the references.
        # Default frames (when SaveAllFrames == 0)
        fsp_frame = 1
        fmp_frame = 2
        # If the method is labeled FqFm
        if 'FqFmFrameFsp' in metadata and metadata["SaveAllFrames"] != "0":
            fsp_frame = int(metadata["FqFmFrameFsp"]) + 1
            fmp_frame = int(metadata["FqFmFrameFmp"]) + 1
        # If the method is labeled LtOjip
        if 'LtOjipFrameFsp' in metadata and metadata["SaveAllFrames"] != "0":
            fsp_frame = int(metadata["LtOjipFrameFsp"]) + 1
            fmp_frame = int(metadata["LtOjipFrameFmp"]) + 1
        frame_labels[0] = "Flight"
        frame_labels[fsp_frame] = 'Fp'
        frame_labels[fmp_frame] = 'Fmp'
        psl = xr.DataArray(
            data=img_cube[..., None],
            dims=('x', 'y', 'frame_label', 'measurement'),
            coords={'frame_label': frame_labels,
                    'frame_num': ('frame_label', frame_nums),
                    'measurement': ['t1']},
            name='ojip_light'
        )
        psl.attrs["long_name"] = "OJIP light-adapted measurements"
        ps.add_data(psl)

        _debug(visual=ps.ojip_light.squeeze('measurement', drop=True),
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_PSL-frames.png"),
               col='frame_label',
               col_wrap=int(np.ceil(ps.ojip_light.frame_label.size / 4)))


def _process_pmd_data(ps, metadata, container_client, kwargs):
    """
    Create an xarray DataArray for a PMD dataset.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    if 'pmd_blob_name' in kwargs:
        img_cube, frame_labels, frame_nums = _read_dat_file(dataset="PMD",
                                                            container_client=container_client,
                                                            blob_path=kwargs['pmd_blob_name'],
                                                            height=int(metadata["ImageRows"]),
                                                            width=int(metadata["ImageCols"]))
        frame_labels = ["Fdark", "F0", "Fm", "Fs"]
        pmd = xr.DataArray(
            data=img_cube[..., None],
            dims=('x', 'y', 'frame_label', 'measurement'),
            coords={'frame_label': frame_labels,
                    'frame_num': ('frame_label', frame_nums),
                    'measurement': ['t0']},
            name='pam_dark'
        )
        pmd.attrs["long_name"] = "pam dark-adapted measurements"
        ps.add_data(pmd)

        _debug(visual=ps.pam_dark.squeeze('measurement', drop=True),
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_PMD-frames.png"),
               col='frame_label',
               col_wrap=int(np.ceil(ps.pam_dark.frame_label.size / 4)))


def _process_pml_data(ps, metadata, container_client, kwargs):
    """
    Create an xarray DataArray for a PML dataset.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    if 'pml_blob_name' in kwargs:
        img_cube, frame_labels, frame_nums = _read_dat_file(dataset="PML",
                                                            container_client=container_client,
                                                            blob_path=kwargs['pml_blob_name'],
                                                            height=int(metadata["ImageRows"]),
                                                            width=int(metadata["ImageCols"]))
        frame_labels = ["Flight", "Fp", "Fmp", "Fs"]
        pml = xr.DataArray(
            data=img_cube[..., None],
            dims=('x', 'y', 'frame_label', 'measurement'),
            coords={'frame_label': frame_labels,
                    'frame_num': ('frame_label', frame_nums),
                    'measurement': ['t0']},
            name='pam_light'
        )
        pml.attrs["long_name"] = "pam light-adapted measurements"
        ps.add_data(pml)

        _debug(visual=ps.pam_light.squeeze('measurement', drop=True),
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_PML-frames.png"),
               col='frame_label',
               col_wrap=int(np.ceil(ps.pam_light.frame_label.size / 4)))


def _process_chl_data(ps, metadata, container_client, kwargs):
    """
    Create an xarray DataArray for a CHL dataset.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    if 'chl_blob_name' in kwargs:
        img_cube, frame_labels, _ = _read_dat_file(dataset="CHL",
                                                   container_client=container_client,
                                                   blob_path=kwargs['chl_blob_name'],
                                                   height=int(metadata["ImageRows"]),
                                                   width=int(metadata["ImageCols"]))
        frame_labels = ["Fdark", "Chl"]
        chl = xr.DataArray(
            data=img_cube,
            dims=('x', 'y', 'frame_label'),
            coords={'frame_label': frame_labels},
            name='chlorophyll'
        )
        chl.attrs["long_name"] = "chlorophyll measurements"
        ps.add_data(chl)

        _debug(visual=ps.chlorophyll,
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_CHL-frames.png"),
               col='frame_label',
               col_wrap=int(np.ceil(ps.chlorophyll.frame_label.size / 4)))


def _process_spc_data(ps, metadata, container_client, kwargs):
    """
    Create a Spectral_data object for the SPC and CLR datasets.

    Inputs:
        ps       = PSII_data instance
        metadata = INF file metadata dictionary
        container_client = Azure Container client
        kwargs = dictionary with blob paths

    :param ps: plantcv.plantcv.classes.PSII_data
    :param metadata: dict
    """
    img_cubes = []
    wavelengths = []
    rgb = None
    if 'clr_blob_name' in kwargs:
        rgb_cube, _, _ = _read_dat_file(dataset="CLR",
                                        container_client=container_client,
                                        blob_path=kwargs['clr_blob_name'],
                                        height=int(metadata["ImageRows"]),
                                        width=int(metadata["ImageCols"]))
        img_cubes.append(rgb_cube)
        wavelengths += [640, 550, 475]
        rgb = img_as_ubyte(rgb_cube[:, :, [2, 1, 0]])
    if 'spc_blob_name' in kwargs:
        spc_cube, _, _ = _read_dat_file(dataset="SPC",
                                        container_client=container_client,
                                        blob_path=kwargs['spc_blob_name'],
                                        height=int(metadata["ImageRows"]),
                                        width=int(metadata["ImageCols"]))
        img_cubes.append(spc_cube)
        wavelengths += [540, 710, 770]
        if rgb is None:
            rgb = img_as_ubyte(spc_cube)

    if len(img_cubes) > 0:
        if len(img_cubes) == 2:
            # Concatenate the images on the depth/spectral (z) axis
            array_data = np.concatenate(img_cubes, axis=2)
        else:
            array_data = img_cubes[0]

        # sort all wavelengths
        wavelengths = np.array(wavelengths)
        ind = np.argsort(wavelengths)
        wavelengths = wavelengths[ind]

        wavelength_dict = dict()
        for (idx, wv) in enumerate(wavelengths):
            wavelength_dict[wv] = float(idx)

        # sort array_data based on wavelengths
        array_data = array_data[:, :, ind]
        # Scale the array data to 0-1 by dividing by the maximum data type value
        array_data = (array_data / np.iinfo(array_data.dtype).max).astype(np.float32)

        # Create a Spectral_data object
        rows, columns = array_data.shape[0:2]
        multispec = Spectral_data(array_data=array_data,
                                  max_wavelength=float(max(wavelengths)),
                                  min_wavelength=float(min(wavelengths)),
                                  max_value=float(np.amax(array_data)),
                                  min_value=float(np.amin(array_data)),
                                  d_type=array_data.dtype,
                                  wavelength_dict=wavelength_dict,
                                  samples=columns, lines=rows, interleave="NA",
                                  wavelength_units="nm", array_type="multispectral",
                                  pseudo_rgb=rgb, filename="NA", default_bands=None)

        ps.spectral = multispec

        _debug(visual=ps.spectral.pseudo_rgb,
               filename=os.path.join(params.debug_outdir, f"{str(params.device)}_spectral-RGB.png"))


# def _dat_filepath(dataset, datapath, filename):
#     """
#     Create the filepath to a DAT file based on the INF filename.

#     Inputs:
#         dataset  = dataset key (PSD, PSL, SPC, CHL, CLR)
#         datapath = path to the dataset (basepath of the INF file)
#         filename = INF filename

#     Returns:
#         bin_filepath = fully-qualified path to the DAT file

#     :param dataset: str
#     :param datapath: str
#     :param filename: str
#     :return bin_filepath: str
#     """
#     filename_components = filename.split("_")
#     # Find corresponding bin img filepath based on .INF filepath
#     # replace header with bin img type
#     filename_components[filename_components.index('HDR')] = dataset
#     bin_filenames = "_".join(filename_components)
#     bin_filename = bin_filenames.replace(".INF", ".DAT")
#     bin_filepath = os.path.join(datapath, bin_filename)

#     return bin_filepath


def _read_dat_file(dataset, container_client, blob_path, height, width):
    """
    Read raw data from DAT file.

    Inputs:
        dataset          = dataset key (PSD, PSL, SPC, CHL, CLR)
        container_client = azure container client
        blob_path        = fully-qualified path to the DAT file
        height           = height (rows) of the images
        width            = width (columns) of the image

    Returns:
        img_cube     = raw data cube in NumPy shape
        frame_labels = list of labels for each frame
        frame_nums   = the number of frames

    :param dataset: str
    :param container_client: Azure ContainerClient
    :param blob_path: str
    :param height: int
    :param width: int
    :return img_cube: numpy.ndarray
    :return frame_labels: list
    :return frame_numbs: int
    """
    print(f'Compiling: {dataset}')
    # Dump in bin img data
    blob_client = container_client.get_blob_client(blob_path)
    stream = blob_client.download_blob()
    buffer = stream.readall()
    raw_data = np.frombuffer(buffer, np.uint16, -1)
    # Reshape, numpy shaped
    img_cube = raw_data.reshape(int(len(raw_data) / (height * width)), width, height).transpose((2, 1, 0))

    # Calculate frames of interest and keep track of their labels. labels must be unique across all measurements
    frame_labels = [(dataset + str(i)) for i in range(img_cube.shape[2])]
    frame_nums = np.arange(img_cube.shape[2])

    return img_cube, frame_labels, frame_nums
