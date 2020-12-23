import numpy as np


def species_lst_to_arr(species: list[int], n_species: int = 999) -> np.ndarray:
    """Cast sparse list of species FIA numbers to array for cosine comparison

    Parameters
    ----------
    species : list-like
        List of species labels (integers)
    n_species : int
        Maximum number of species

    Returns
    -------
    array
       Sparse array of species, zeros where species is not in input list.
    """
    arr = np.zeros(n_species)
    arr[species] = 1
    return arr
