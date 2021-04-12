from carbonplan_forest_offsets.data import get_filesystem

if __name__ == '__main__':
    '''syncs rFIA run outputs to `cat`'''
    fs = get_filesystem()
    fs.put(
        '/home/jovyan/rfia/processed_data/no_buffer_biomass/',
        'carbonplan-retro/rfia',
        recursive=True,
        overwrite=True,
    )
    fs.put(
        '/home/jovyan/rfia/processed_data/no_buffer_biomass_all/',
        'carbonplan-retro/rfia_all',
        recursive=True,
        overwrite=True,
    )
    fs.put(
        '/home/jovyan/rfia/processed_data/297_by_ecosection.csv',
        'carbonplan-retro/results/297_by_ecosection.csv',
        overwrite=True,
    )
