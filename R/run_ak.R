#############################################
FIA_DIR <- '/home/jovyan/rfia/data/'
out_dir <- '/home/jovyan/rfia/processed_data/no_buffer_biomass'

library(rgeos)
library(rFIA)
library(rgdal)
library(rjson)
library(dplyr)

cores <- 12

supersections <- readOGR('ak_shapefiles/')

assessment_areas <- fromJSON(file='ak_assessment_areas.json')

process_assessment_area <- function(assessment_area) {

  supersection <- subset(supersections, Assessment == assessment_area$supersection_name)
  print(assessment_area$assessment_area_id)
  clipped_fia <- clipFIA(readFIA(states = assessment_area$postal_codes,
                                 dir = FIA_DIR, nCores = cores,
                                 tables = c('PLOT', 'TREE', 'COND', 'POP_PLOT_STRATUM_ASSGN','POP_ESTN_UNIT', 'POP_EVAL',
                                            'POP_STRATUM', 'POP_EVAL_TYP', 'POP_EVAL_GRP')
  ), matchEval = TRUE, mostRecent = FALSE, nCores=cores)
  print('analysis')
  clipped_fia$COND <- clipped_fia$COND %>%
    mutate(site = case_when(is.na(SITECLCD) ~ NA_character_,
                          SITECLCD %in% 1:4 ~ 'high',
                          TRUE ~ 'low'))

  bio <- biomass(clipped_fia,
                 grpBy=c(site, FORTYPCD),
                 areaDomain = OWNGRPCD == 40, #& FORTYPCD %in% assessment_area$fortypcds,
                 treeType='live',
                 method = 'TI',
                 polys= supersection,
                 variance=TRUE,
                 component = 'AG',
                 totals=TRUE,
                 nCores=12)

  bio_subset <- subset(bio, CARB_TOTAL > 0, na.rm=TRUE)

  fn <- paste(assessment_area$assessment_area_id, '.csv', sep='')
  write.csv(bio_subset, file=file.path(out_dir, fn))
}


for (assessment_area in assessment_areas) {
  process_assessment_area(assessment_area)
}
