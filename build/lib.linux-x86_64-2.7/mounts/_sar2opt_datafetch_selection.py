import fetchme
import mapme
import dateutil.parser
import os

obj = fetchme.Scihub()

#  --- define filenames to download
selection = {}
# selection['aug'] = ['S1B_IW_SLC__1SDV_20170822T161848_20170822T161915_007055_00C6D5_D030', 'S2A_MSIL2A_20170816T095031_N0205_R079_T34UEE_20170816T095031']
# selection['sept'] = ['S1A_IW_SLC__1SDV_20170921T161843_20170921T161910_018476_01F1E3_5EAE', 'S2A_MSIL2A_20170909T093031_N0205_R136_T34UFB_20170909T093604']
# selection['dec'] = ['S1A_IW_SLC__1SDV_20171226T161842_20171226T161909_019876_021D28_F924', 'S2B_MSIL2A_20171226T094359_N0206_R036_T34UFB_20171226T113723']
# selection['jul'] = ['S1A_IW_SLC__1SDV_20170730T050048_20170730T050116_017696_01DA23_E924', 'S2A_MSIL2A_20170730T100031_N0205_R122_T33UXS_20170730T100535']

selection['test_imges'] = ['S1B_IW_SLC__1SDV_20170729T050754_20170729T050821_006698_00BC7D_EB40', ' S2A_MSIL2A_20170730T100031_N0205_R122_T33UXU_20170730T100535']

download_zip = 1
download_quicklook = 1
download_dir = '/home/sebastien/DATA/data_sar2opt/raw/poland/'
plot_downloadedPairs = 1

for k, v in selection.items():

    filename_s1 = selection[k][0]
    filename_s2 = selection[k][1]

    # --- query scihub
    s1 = obj.scihub_search(filename=filename_s1 + '*')
    s2 = obj.scihub_search(filename=filename_s2 + '*')

    # --- get product metadata
    p1 = s1[0]
    p2 = s2[0]
    t1 = dateutil.parser.parse(p1.metadata.beginposition)
    t2 = dateutil.parser.parse(p2.metadata.beginposition)
    dt = abs(t1 - t2)
    t1_iso = t1.strftime('%Y%m%dT%H%M%S')
    t2_iso = t2.strftime('%Y%m%dT%H%M%S')
    s1_wkt = p1.metadata.footprint
    s2_wkt = p2.metadata.footprint

    # --- download product if area of intersection above threshold
    if download_zip or download_quicklook:

        # --- create result folder
        pair_name = 'S1-' + t1_iso + '_S2-' + t2_iso
        dir_pair = download_dir + pair_name
        if not os.path.exists(dir_pair):
            os.makedirs(dir_pair)
            print 'Directory "' + dir_pair + '" created'

        # --- plot pair
        if plot_downloadedPairs:
            # plot_wkt(wkt2plot=[s1_wkt, s2_wkt], f_out=pair_name, p_out=dir_pair)
            mapme.plot_wkt(wkt2plot=[s1_wkt, s2_wkt], plot_country='Poland', f_out=pair_name, p_out=dir_pair)

        # --- save wkt file
        text_file = open(dir_pair + "/S1_wkt.txt", "w")
        text_file.write(p1.metadata.footprint)
        text_file.close()

        text_file = open(dir_pair + "/S2_wkt.txt", "w")
        text_file.write(p2.metadata.footprint)
        text_file.close()

        # --- download pair
        if download_zip:
            print('    downloading: ' + p1.metadata.title)
            p1.getFullproduct(download_dir=dir_pair)
            p2.getFullproduct(download_dir=dir_pair)

        if download_quicklook:
            print('    downloading: ' + p2.metadata.title)
            p1.getQuicklook(download_dir=dir_pair)
            p2.getQuicklook(download_dir=dir_pair)
