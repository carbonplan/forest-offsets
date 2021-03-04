#############################################
FIA_DIR <- '/home/jovyan/rfia/data/'
out_dir <- '/home/jovyan/rfia/processed_data/'

#install.packages('rgeos')
library(rFIA)
library(rgdal)
library(rjson)
library(dplyr)



assessment_areas <- fromJSON(file='/home/jovyan/lost+found/rfia_assessment_areas_subset.json')
process_assessment_area <- function(assessment_area) {

  print(assessment_area$assessment_area_id)
  clipped_fia <- clipFIA(readFIA(states = assessment_area$postal_codes,
                                 dir = FIA_DIR, nCores = 12,
                                 tables = c('PLOT', 'TREE', 'COND', 'POP_PLOT_STRATUM_ASSGN','POP_ESTN_UNIT', 'POP_EVAL', 'POP_STRATUM', 'POP_EVAL_TYP', 'POP_EVAL_GRP')
                                ),
                         matchEval = TRUE, mostRecent = FALSE, nCores=12)
  print('analysis')
  clipped_fia$COND <- clipped_fia$COND %>%
    mutate(site = case_when(is.na(SITECLCD) ~ NA_character_,
                          SITECLCD %in% 1:4 ~ 'high',
                          TRUE ~ 'low'))



  bio <- biomass(clipped_fia,
                 grpBy=c(site, ECOSUBCD),
                 areaDomain = OWNGRPCD == 40 & FORTYPCD %in% assessment_area$fortypcds,
                 treeType='live',
                 method = 'TI',
                 variance=TRUE,
                 component = 'AG',
                 totals=TRUE,
                 nCores=12)

  bio_subset <- subset(bio, CARB_TOTAL > 0, na.rm=TRUE)
  fn <- paste(assessment_area$assessment_area_id, '_by_ecosection.csv', sep='')
  write.csv(bio_subset, file=file.path(out_dir, fn))
  gc()
}


for (assessment_area in assessment_areas[89]) {
  process_assessment_area(assessment_area)
}
