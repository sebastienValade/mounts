
# === FETCHME snippets

import fetchme
obj = fetchme.Scihub()

# --- ex: search by file name
p = obj.scihub_search(filename='S1*IW*SLC*')
print p[0].metadata

# --- ex: search with multiple options
optn = dict(
    platformname='Sentinel-1',
    producttype='SLC',
    sensoroperationalmode='IW',
    footprint='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))',
    maxrecords=5)
p = obj.scihub_search(**optn)
print p[0].metadata.title

# --- ex: download quicklook (for Sentinel-1 IW/SLC products)
p[0].getQuicklook()

# --- ex: download full product
p[0].getFullproduct()

# -----------------------------------------------------------------------------------------------------------------------------

# === SNAPME snippets

import snapme as gpt

# --- dinsar processing chain
main_path = '/home/khola/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.zip'
subordinate_path = '/home/khola/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.zip'
m = gpt.read_product(path_and_file=main_path) 						# open main product
s = gpt.read_product(path_and_file=subordinate_path)							# open subordinate product
m = gpt.topsar_split(m, subswath='IW2', polarisation='VV')				# select swath main
s = gpt.topsar_split(s, subswath='IW2', polarisation='VV') 				# select swath subordinate
m = gpt.apply_orbit_file(m) 											# apply orbit file main
s = gpt.apply_orbit_file(s)												# apply orbit file subordinate
p = gpt.back_geocoding(m, s)			 								# coregister main/subordinate
p = gpt.interferogram(p) 												# interferogram generation
p = gpt.deburst(p)														# deburst
p = gpt.topo_phase_removal(p) 											# topographic phase removal
p = gpt.goldstein_phase_filtering(p) 									# phase filtering
bands = gpt.get_bandnames(p) 											# get bands in product
p = gpt.terrain_correction(p, bands[3]) 								# geocoding
p = gpt.subset(p, geoRegion='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))')
gpt.plotBands(p, bands[3])												# geocoding
