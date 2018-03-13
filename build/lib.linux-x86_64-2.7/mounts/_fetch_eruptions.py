import fetchme
import pandas as pd
import datetime
import os

# --- load eruption list
p = '/home/sebastien/Documents/MOUNTS/VARIOUS/VARIOUS/GVP_databases/DB_eruptions/'
f = 'GVP_Eruption_Results_2014-2017.xlsx'
df = pd.read_excel(p + f)

p_out = '/home/sebastien/DATA/data_eruptions/'

lat = df.Latitude
lon = df.Longitude
name = df['Volcano Name']
nameshort = df['Volcano Name'].apply(lambda x: x.lower().replace(' ', '').replace(',', ''))
ymd = df[['Start Year', 'Start Month', 'Start Day']]
date = ymd.apply(lambda x: datetime.datetime(*x), axis=1)
date_str = date.apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))


# --- search
obj = fetchme.Scihub()

for i, r in df.iterrows():

  name = r['Volcano Name']
  nameshort = name.lower().replace(' ', '').replace(',', '')
  eruption_ymd = date[i].strftime('%Y%m%d')
  # eruption_ymd = '-'.join([str(r['Start Year']), str(r['Start Month']), str(r['Start Day'])])

  print '---', name, '-'.join([str(r['Start Year']), str(r['Start Month']), str(r['Start Day'])])

  # --- create result folder
  eruption_name = nameshort + '_' + eruption_ymd
  dir_eruption = p_out + eruption_name
  if not os.path.exists(dir_eruption):
    os.makedirs(dir_eruption)
    print 'Directory "' + dir_eruption + '" created'

  # --- set product date based on eruption date
  ti = datetime.datetime(r['Start Year'], r['Start Month'], r['Start Day'])
  ti_str = ti.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  tf = datetime.datetime(r['End Year'], r['End Month'], r['End Day'])
  tf_str = tf.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  tf_30d = ti + datetime.timedelta(days=30)
  tf_30d_str = tf.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  # dt = tf - ti
  # thresh_hours = 24 * 30
  # if dt.days * 86400 + dt.seconds < thresh_hours * 3600:
  #     tf_str_safe = ti + datetime.timedelta(days=30)
  # else:
  #     tf_str_safe = tf_str

  beginposition = "[{} TO {}]".format(ti_str, tf_30d_str)

  # --- set product footprint based on eruption location
  footprint = [r.Latitude, r.Longitude]

  # --- search sentinel-1 products
  # ==================================================================================
  s1_optn = {'filename': 'S1*IW*SLC*',
             'footprint': footprint,
             'beginposition': beginposition,
             'maxrecords': 25,
             'orderby': 'beginposition asc'  # => 1st product = closest to start time
             }
  print '   s1 products:'
  s1 = obj.scihub_search(**s1_optn)
  for p1 in s1:
    # p1.getQuicklook(download_dir=dir_eruption+'/quicklooks/')
    p1.getQuicklook()

  # --- search sentinel-2 products
  # ==================================================================================
  s2_optn = {'filename': 'S2*MSIL1C*',
             'footprint': footprint,
             'beginposition': beginposition,
             'maxrecords': 25,
             'orderby': 'beginposition asc'  # => 1st product = closest to start time
             }
  print '   s2 products:'
  s2 = obj.scihub_search(**s2_optn)
  for p2 in s2:
    p2.getQuicklook()
    # p2.getQuicklook(download_dir=dir_eruption)

  import sys
  sys.exit()
