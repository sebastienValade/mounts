import tasks
import fetchme
import utilme

query = 0
download = 0
process = 1

target_name = 'ertaale'

# === QUERY
if query:

  # NB: if dict optn_sX empty => ignores query

  optn = dict(
      target_name=target_name,
      acq_tspan=['2017-01-01', '2018-04-01'],
      print_queryresult=1,  # >> prints result for each month of timespan
      download_pnode=None,  # 'quicklook',
      download_dir=None,
  )

  optn_s1 = dict(
      filename='S1*IW*SLC*',
      filter_products=1,  # => filter products if acquired at same minute, keep only products where intersection percentage with footprint is max)
      download_pnode=optn['download_pnode'],
      download_dir=optn['download_dir'],
      maxrecords=100,
  )

  optn_s2 = dict(
      # filename='S2*MSIL1C*',  # 'S2*MSIL2A*',
      # # 'cloudcoverpercentage': '[0 TO 2]',
      # filter_products=1,  # => filter products if acquired at same minute, keep only tiles where intersection percentage with footprint is max)
      # download_pnode=optn['download_pnode'],
      # download_dir=optn['download_dir'],
      # maxrecords=100,
  )

  p = tasks.query(optn, optn_s1, optn_s2)
  # p[1].getQuicklook()
  # print p[1].metadata.title
  print('=> products found in scihub:')
  fetchme.Scihub().print_product_title(p)

  print('=> products found in scihub which are not in DB_MOUNTS:')
  dbo = utilme.Database()
  p_in, p_new = dbo.dbmounts_isproduct_archived(p)
  pnew_title = fetchme.Scihub().print_product_title(p_new)


# === DOWNLOAD + LOAD to archive
if download:
  download_pnew = 1  # >> download new products only (else calculates md5sum and downloads if inconsistent)
  download_pidx = None  # [2, 3, 4]    # >> None=>download full product list; [0]=>download first product in list
  download_pnode = 'fullproduct'   # >> None | 'quicklook' | 'fullproduct'
  download_rootdir = '/home/sebastien/DATA/data_satellite/'
  load_2dbarchive = 1

  # --- select product(s) to download within productlist
  if download_pnew:
    p = p_new

  if download_pidx:
    p = [p[i] for i in download_pidx]

  print('Products to download:')
  fetchme.Scihub().print_product_title(p)

  # => download list of products and store in DB_mounts.archive
  tasks.download(p,
                 download_pnode=download_pnode,
                 download_rootdir=download_rootdir,
                 load_2dbarchive=load_2dbarchive,
                 target_name=target_name)

# === PROCESS
if process:
  pcss_dinsar = 1
  pcss_sar = 0
  pcss_nir = 0

  cfg_productselection = dict(
      target_name=target_name,  # = compulsory to get processing options
      acqstarttime='>2017-09-01 <2017-12-01',
      # title=['S2A_MSIL1C_20171225T075211_N0206_R092_T37PFR_20171225T095830', 'S2B_MSIL1C_20171230T074319_N0206_R092_T37PFR_20171230T095744'],
      # title=pnew_title,
  )

  ignore_processedProducts = 1  # >> avoid reprocessing products already processed
  quit_after_querydb = 0
  print_sqlQuery = 0
  print_sqlResult = 1
  pathout_root = '/home/sebastien/DATA/data_mounts/'
  store_result2db = 1

  tasks.process(pcss_sar=pcss_sar,
                pcss_dinsar=pcss_dinsar,
                pcss_nir=pcss_nir,
                cfg_productselection=cfg_productselection,
                ignore_processedProducts=ignore_processedProducts,
                store_result2db=store_result2db,
                print_sqlQuery=print_sqlQuery,
                print_sqlResult=print_sqlResult,
                quit_after_querydb=quit_after_querydb,
                pathout_root=pathout_root)
