import os
import pandas as pd
from shapely.wkt import loads
import geopandas as gpd
from geopandas import GeoSeries, GeoDataFrame
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


def plot_wkt(wkt2plot=None, f_out=None, p_out=None, plot_world=True, plot_country=None):

    if wkt2plot is None:
        return

    # --- construct GeoDataFrame
    geometry = [loads(wkt) for wkt in wkt2plot]
    df = pd.DataFrame()
    gdf = GeoDataFrame(df, geometry=geometry)

    # --- construct GeoSeries
    #gs = GeoSeries(geometry)

    # --- plot
    f, ax = plt.subplots(1)

    if plot_world:
        world.plot(ax=ax, facecolor='lightgray', edgecolor='gray')

    if plot_country is not None:
        country = world[world.name == plot_country]
        if country.empty:
            print('Country named "{}" was not found.'.format(plot_country))
        else:
            country_bounds = country.geometry.bounds
            country.plot(ax=ax, facecolor='lightgray', edgecolor='red')
            plt.xlim([country_bounds.minx.min() - 2, country_bounds.maxx.max() + 2])
            plt.ylim([country_bounds.miny.min() - 2, country_bounds.maxy.max() + 2])

    gdf.plot(ax=ax, cmap='Set2', alpha=0.1, edgecolor='black')
    # gs.plot(ax=ax, cmap='Set2', alpha=0.7, edgecolor='black')

    ax.set_axis_off()
    plt.axis('equal')

    if not country.empty:
        plt.xlim([country_bounds.minx.min() - 2, country_bounds.maxx.max() + 2])
        plt.ylim([country_bounds.miny.min() - 2, country_bounds.maxy.max() + 2])

    plt.show()

    # --- save png
    if f_out is not None:

        if p_out is None:
            p_out = '../data/'
        else:
            p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

        plt.savefig(p_out + f_out)
