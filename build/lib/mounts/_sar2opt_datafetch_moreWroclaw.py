import fetchme
from osgeo import ogr
import dateutil.parser

obj = fetchme.Scihub()

wroclaw_s2 = 'S2A_MSIL2A_20170730T100031_N0205_R122_T33UXS_20170730T100535'

# --- search s2 product used in paper
s2 = obj.scihub_search(filename=wroclaw_s2 + '*', cloudcoverpercentage='[0 TO 2]')
p2 = s2[0]
s2_wkt = p2.metadata.footprint
s2_poly = ogr.CreateGeometryFromWkt(s2_wkt)
s2_area = s2_poly.GetArea()

# --- search s1 products overlapping s1
s1_optn = {'filename': 'S1*IW*SLC*',
           'footprint': s2_wkt,
           'ingestiondate': "[2017-07-01T00:00:00.000Z TO 2017-08-01T00:00:00.000Z]",
           'maxrecords': 100
           }
s1 = obj.scihub_search(**s1_optn)

# --- loop through s1 products
for p1 in s1:
    # print('  | ' + p1.metadata.title)
    s1_wkt = p1.metadata.footprint
    s1_poly = ogr.CreateGeometryFromWkt(s1_wkt)
    s1_area = s1_poly.GetArea()

    # --- calculate intersection between s1 product and s2
    itsc = s2_poly.Intersection(s1_poly)
    itsc_wkt = itsc.ExportToWkt()
    itsc_area = itsc.GetArea()
    overlap_pct = (itsc_area / s2_area) * 100
    if overlap_pct < 90:
        # print 's2 product not inside polish borders => discarded'
        continue

    print '--> ' + p1.metadata.title
    t1 = dateutil.parser.parse(p1.metadata.beginposition)
    t2 = dateutil.parser.parse(p2.metadata.beginposition)
    dt = abs(t1 - t2)
    print '   dt = ' + str(dt)
