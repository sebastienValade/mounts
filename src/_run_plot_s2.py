import os
import glob
import snapme as gpt


path_dir = '/home/sebastien/DATA/data_satellite/ertaale_S2/'
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
subset_bounds = 'POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))'  # >> ertaale
# subset_bounds = 'POLYGON((29.2424672842 -1.5146708235, 29.2563611269 -1.5146708235, 29.2563611269 -1.5291925429, 29.2424672842 -1.5291925429, 29.2424672842 -1.5146708235))' # >> nyiragongo

# f = glob.glob(os.path.join(path_dir, '') + '*S2A_MSIL1C*.zip')
f = glob.glob(os.path.join(path_dir, '') + '*.zip')

for k, fpath in enumerate(f):

    fname = os.path.basename(fpath)
    title = os.path.splitext(fname)[0]

    # if title == 'S2A_OPER_PRD_MSIL1C_PDMC_20160805T185025_R092_V20160802T075556_20160802T075556':
    #     print 'java fata error ???'
    #     continue

    print '---'
    print title

    try:
        print fpath
        p = gpt.read_product(path_and_file=fpath)

        p = gpt.resample(p, referenceBand='B2')

        # p = gpt.subset(p, **subset_bounds)
        p = gpt.subset(p, geoRegion=subset_bounds)

        acqdate_str = p.getMetadataRoot().getElement('Level-1C_DataStrip_ID').getElement('General_Info').getElement('Datastrip_Time_Info').getAttributeString('DATASTRIP_SENSING_START')

        print(acqdate_str)

        bname_red = 'B12'
        bname_green = 'B11'
        bname_blue = 'B8A'
        f_out = title + '_' + bname_red + bname_green + bname_blue + '.png'
        gpt.plotBands_rgb(p, bname_red=bname_red, bname_green=bname_green, bname_blue=bname_blue, f_out=f_out)

        bname_red = 'B4'
        bname_green = 'B3'
        bname_blue = 'B2'
        f_out = title + '_' + bname_red + bname_green + bname_blue + '.png'
        gpt.plotBands_rgb(p, bname_red=bname_red, bname_green=bname_green, bname_blue=bname_blue, f_out=f_out)

        p.dispose()

    except:
        print 'ERROR reading ' + title
        continue
