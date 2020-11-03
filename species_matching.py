from fuzzywuzzy import process
import numpy as np
import pandas as pd




def species_lst_to_arr(lst, n_species=999):
    """
    cast sparse list of species FIA numbers to array for cosine comparison
    pretty sure all NA species are < 999
    """
    arr = np.zeros(n_species)
    arr[lst] = 1
    return arr


