# MOUNTS
## Monitoring Unrest from Space

---

### Code examples

1. Fetch product from SciHub (**fetchme**)
    ```python
    import fetchme
    obj = fetchme.Scihub()
    obj.query_auth('sebastien.valade', 'wave*worm')

    #--- parse options from kwargs
    obj.scihub_search(filename='S1*')

    #--- parse options from dict
    optns = {'filename':'S1A*', 'maxrecords':5}
    obj.scihub_search(**optns)

    #--- parse options from yaml configuration file
    obj.scihub_search(configfile='./conf/_config_ertaale.yml')

    #-- parse options from loaded yaml configration file
    obj.read_configfile('./conf/_config_ertaale.yml')
    obj.scihub_search()
    ```

2. Process product using SNAP (**run_processing.py**)

    ``` python
    import snapme as gpt

    # --- read master product
    master_abspath = oldprod.prod_abspath
    m = gpt.read_product(path_and_file=master_abspath)

    # --- read slave product
    s = gpt.read_product(path_and_file=slave_abspath)

    # --- split product
    m = gpt.topsar_split(m, subswath='IW2')
    s = gpt.topsar_split(s, subswath='IW2')

    # --- apply orbit file
    m = gpt.apply_orbit_file(m)
    s = gpt.apply_orbit_file(s)

    # --- back-geocoding
    p = gpt.back_geocoding(m, s)

    # --- interferogram
    p = gpt.interferogram(p)

    # --- deburst
    p = gpt.deburst(p)

    # --- topographic phase removal
    p = gpt.topo_phase_removal(p)

    # --- phase filtering
    p = gpt.goldstein_phase_filtering(p)

    # --- terrain correction (geocode)
    gpt.get_bandnames(p, print_bands=1)
    sourceBands = ['Phase_VV_11Jan2017_04Feb2017', 'coh_IW2_VV_11Jan2017_04Feb2017']
    p = gpt.terrain_correction(p, sourceBands)

    # --- subset
    p = gpt.subset(p, north_bound=13.53, west_bound=40.63, south_bound=13.64, east_bound=40.735)

    # --- plot
    band = gpt.plotBand(p, sourceBands, cmap=['gist_rainbow', 'binary_r'])

    # Dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    p.dispose()
    ```

2. Set logging behavior 
    ``` python
    import logging
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    ```

