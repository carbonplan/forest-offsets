<img
  src='https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png'
  height='48'
/>

# Forest Offsets Archive

The code used to generate intermediate and final outputs, as well as examples of how to read the input datafiles, is contained in a separate [Github repository](https://github.com/carbonplan/forest-offsets)

## data artifacts

A brief description of the inputs, intermediate data products, and final outputs included in this archive.

### inputs

#### FIA
- `raw_fia.zip`:  Compressed archive of FIA data used to drive the rFIA analysis. TREE biomass estimates have been modified in the PNW work unit to use regional biomass estimates, as opposed to the national estimate. Similar data (pulled a few months prior) were used for training the nearest neighbors classifier. Those exact data are described in the [CarbonPlan forest-risks repository](https://github.com/carbonplan/forest-risks).
- `fia-long`: Estimates of aboveground biomass for all conditions in the states of California and Oregon, which are combined with PRISM data to create climatologically-informed estimates of the spatial patterns of carbon storage. Data produced via the methods described in the [CarbonPlan forest-risks repository](https://github.com/carbonplan/forest-risks).
- `PRISM`: 30 year annual average temperature (tmean) and precipitation (ppt), regridded to 4km for both the continental United States and Alaska. CONUS data spans 1981-2010 and AK data spans 1971-2000. More details available from the (PRISM website)[https://prism.oregonstate.edu/normals/]
- `forest-offsets-database`: Directory containing a copy of the [California improved forest management offset project database (Version 1.0.0)](https:/doi.org/http://doi.org/10.5281/zenodo.4630684).

#### ancillary
- `assessment_area_forest_type_codes.json`: A list of all FIA forest type codes assigned to each assessment area. Mappings derived from [California Air Resources Board provided data](https://ww2.arb.ca.gov/our-work/programs/compliance-offset-program/compliance-offset-protocols/us-forest-projects/2015/common-practice-data).
- `supersections.json`: Geographic regions, created by combining together two or more ECOMAP 2007 ecosections.
- `ecomap_sections.json`: ECOMAP 2007 ecosection boundaries. The Climate Action Reserve and California Air Resources Board forest offset protocols aggregated these shapes to create a custom set of “supersections.”
- `states.json`: Boundaries of US states, originally from [Natural Earth Data](https://www.naturalearthdata.com).

### intermediate

- `rFIA`: standing live aboveground biomass estimates from [running rFIA](https://github.com/carbonplan/forest-offsets/blob/main/R).
assessment_areas: Directory containing per assessment area outputs from rFIA that do not aggregate by site class. Filenames adhere to the following naming convention: {{ assessment_area_id }}.csv.
- `297_by_ecosection.csv`: per ecosubsection (ECOSUBCD) estimates of common practice for the “mixed conifer assessment area” (code: 297) of the Southern Cascades supersection.
- `Classification`: outputs of the [radius-neighbors classifier](https://github.com/carbonplan/forest-offsets/blob/main/notebooks/0y_classify_projects.ipynb)
- `radius_neighbor_params.json`: for each supersection, the radius parameter that maximized classification accuracy when training the classifier using FIA data and 5-fold, cross-validation.
- `classifications.json`: probability of each project-assessment area being classified as a specific forest type code, based on project reported species data.

### results

Processed and subset data underlying each main text and supplemental figure of Badgley et al. (2021). Full code to produce the figures is [here](https://github.com/carbonplan/forest-offsets-paper/).

- `reclassification-crediting-error.json`: Number of offset credits each project would receive using our alternate, ecologically robust estimate of common practice. Each project has 1,000 estimates of crediting error, based on Monte Carlo error propagation of rFIA-derived estimates of variance in mean carbon per acre.
- `79.json`: geojson with gridded estimates of standing live aboveground carbon using local climatic conditions and calculating the average carbon based on FIA plots with similar climate.
- `southern_cascades_mixed_conifer_by_ecosection.json`: Estimates of standing live aboveground carbon for the three _ecosections_ that constitute the Southern Cascades supersection. Keys represent the ecosection code and values are the rFIA derived estimate of standing live aboveground carbon.
- `classifier_fscores.json`: weighted [F1 scores](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html) describing classifier accuracy per ecosection.
- `reclassification-labels.json`: Human-readable mappings of project species composition to probabilistic estimates of forest type.
- `crediting-verification.json`: key-value store offset credits derived from baseline and project scenarios recorded in the `forest-offset-database` and offset credits awarded by the California Air Resources Board, as recorded in their official issuance table.
- `common-practice-verification.json`:
- `assessment_areas`: key-value store of common practice estimates per assessment area and site class, as re-derived from rFIA compared against the 2015 estimates of common practice used by the California Air Resources Board.
- `projects`: key-value store of common practice on a per project basis, taking into account projected reported acreage per assessment area and site class.

## license
The dataset is licensed using the [CC-BY-4.0](https://choosealicense.com/licenses/cc-by-4.0/) license. Supporting datasets included in this dataset are in the public domain. If you make use of this dataset, we ask you cite the following sources:

## references
- Badgley, G and Freeman, J and Hamman, J J and Haya, B and Trugman, A T and Andereg, W R L and Cullenward, D. “Systematic over-crediting in California’s forest carbon offsets program.”
- Burrill, Elizabeth A., et al., The Forest Inventory and Analysis Database: Database Description and User Guide for Phase 2 (version 8.0) (2018).
- F. Pedregosa, et al., Scikit-learn: Machine Learning in Python. Journal of Machine Learning Research 12, 2825–2830 (2011)
- PRISM Climate Group, Oregon State University, http://prism.oregonstate.edu, created 4 Feb 2004.
- H. Stanke, A. O. Finley, A. S. Weed, B. F. Walters, G. M. Domke, rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database. Environmental Modelling & Software 127, 104664 (2020)

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/forest-offsets/issues/new) or [sending us an email](mailto:hello@carbonplan.org).

## contributors

This database was developed by CarbonPlan staff and the following outside contributors:

- Anna T. Trugman
- Barbara Haya
- Grayson Badgley
- William R. L. Anderegg
