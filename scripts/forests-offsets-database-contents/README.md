<img
  src='https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png'
  height='48'
/>

# California improved forest management offset project database

This archive includes a database of California improved forest management (IFM) offset projects. The database was created by manually digitizing the "offset project data reports" (OPDRs). It includes the following components:

### Offset projects database

`/forest-offsets-database-{VERSION}.{FORMAT}`

The complete projects database provided in `csv` and `json` formats.

We recorded the following attributes for each project:

```json
{
  "id": "Unique project identifier that is always equal to `opr_id`",
  "opr_id": "Unique project identifier assigned by an offset project registry (e.g., CAR, ACR)",
  "arb_id": "Unique project identifier assigned by the California Air Resources Board",
  "name": "Project name as listed in initial OPDR",
  "apd": "Full name of the authorized project designee (APD), if any",
  "opo": "Full name of the offset project owner (OPO)",
  "owners": "List of owners, with project owners being listed in reverse chronological order. In select cases, “owner” differs from opo, as we attempted to map OPOs that were subsidiary (e.g., LLC) or otherwise related entities, to their more commonly known parent entity",
  "developers": "Inferred list of organizations involved in the development (e.g., financing, modeling) of the offset project",
  "attestor": "Name of the individual who signed the OPDR",
  "is_opo": "Boolean flag if the attestor represents the attestor",
  "shape_centroid": "Project latitude(s) and longitude(s) as inferred from ARB provided shapefiles. Shape centroid takes multiple values when the project geometry is a highly fragmented MultiPolygon",
  "supersection_ids": "List of the supersections the project occupies (with a maximum length of two)",
  "acreage": "Project reported acreage",
  "buffer_contribution": "Number of ARBOCs contributed to the forest buffer pool",
  "arbocs": {
    "issuance": "Number of ARBOCs from reporting period one, as reported by the California Air Resources Board issuance table",
    "calculated": "Number of ARBOCs from reporting period one, as inferred from project reported baseline and project scenarios",
    "reported": "Number of ARBOCs from reporting period one, as reported by the project OPDR"
  },
  "carbon": {
    "initial_carbon_stock": "Metric tons of CO2 per acre in aboveground biomass at project inception",
    "common_practice": "Project reported of common practice (metric tons of CO2 per acre)",
    "average_slag_baseline": "100-year average of metric tons of CO2 per acre in aboveground biomass in the project baseline scenario"
  },
  "Baseline": "Individual components of the baseline scenario (e.g., IFM-1), reported in terms of metric tons CO2",
  "rp_1": "Details of the first reporting period, including start and end date, as well as individual components of the project scenario, reported in terms of metric tons of CO2. Also includes project reported estimate of secondary effects and the confidence deduction (e.g., measurement error) of carbon stocks in the project scenario",
  "assessment_areas": "List of all assessment areas in the project. Each record contains the assessment area code, which is a unique numeric identifier of each assessment area, the site class of the assessment area (low, high, or all), and the total project acreage assigned to that assessment area. Assessment areas also contain a list of the species composition of the assessment area. Each species is denoted by its US Forest Service Forest Inventory and Analysis species code and, when possible, we record both the total basal area (per acre) and fractional basal area. Some projects only report species composition for the entire project (across all assessment areas), which we denote using a special assessment area with code 999.",
  "notes": "Pertinent details regarding the provenance of project attributes",
  "comment": "Additional comments about project"
}
```

### Ancillary files

`/ancillary/arboc_issuance_2020-09-09.xlsx`

The California Air Resources Board's Credit Issuance Table from September 9, 2020.

`/ancillary/assessment_area_lookup.csv` and `/ancillary/super_section_lookup.csv`

For expediency in data entry, we assigned each assessment area and supersection an incrementing, unique identifier. Mappings between those ids and the full names used elsewhere in the CARB Forest Offset Protocol are included in `super_section_lookup.csv` and `assessment_area_lookup.csv`

### Project files

`/projects/{OPR_ID}/`

We include the shapefile (`shape.json`) and a Zip file of OPDRs for each project. All project shapefiles have a CRS of EPSG:4326.

## methods

We sourced project data from publicly available offset project data reports (OPDRs) submitted to CARB. We manually transcribed critical project attributes including total project acreage, initial carbon stocks, and the supersections and assessment areas involved in each project. We recorded 100-year average standing live aboveground carbon stocks in project baseline scenarios. For the initial reporting period, we recorded onsite carbon stocks (denoted IFM-1 and IFM-3) and the carbon stocks contained within wood products (IFM-7 and IFM-8), both for the baseline and project scenarios, as well as the project’s reported secondary effects and confidence deduction factors. We also transcribed all reported species with greater than 5% fractional basal area, on a per-assessment-area basis where data were available or else for the entire project.

Missing values mean we were unable to locate that specific project attribute. Unless mentioned in the notes, we made no attempt to impute missing data, thus providing an “as is” snapshot of each project.

## license

The dataset is licensed using the [CC-BY-4.0](https://choosealicense.com/licenses/cc-by-4.0/) license. Supporting datasets included in this dataset are in the public domain. If you make use of this dataset, we ask you cite the following sources:

```
G. Badgley, et al., California improved forest management offset project database (Version 1.0.0) https:/doi.org/http://doi.org/10.5281/zenodo.4630684.
```

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/forest-offsets/issues/new) or [sending us an email](mailto:hello@carbonplan.org).

## contributors

This database was developed by CarbonPlan staff and the following outside contributors:

- Grayson Badgley
- Barbara Haya
