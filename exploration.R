rm(list=ls())

library(aws.s3)
library(dplyr)
library(xlsx)

BUCKET <- "bbrunel"

df <- aws.s3::s3read_using(FUN = read.csv,
                           stringsAsFactors = T,
                           na.strings = c("","NA"),
                           object = "BDD_Label/Enquetes_AC.csv",
                           bucket = BUCKET,
                           opts = list("region" = ""))

df <- df %>% mutate(dateCommissionAC = as.Date(dateCommissionAC),
                    dateCommissionAO = as.Date(dateCommissionAO))
# df <- df %>% 
      # mutate(avisConformite = case_match(statutEnquete,
                                         # "Enquête d'intérêt général et de qualité statistique" ~ 1,
                                         # "Enquête d'intérêt général et de qualité statistique à caractère obligatoire" ~ 1,
                                         # .default = 0))

# renvoie la date correspondant au premier janvier de l'année 'year'

ny <- function(year = 2018) {
  return(as.Date(paste(as.character(year), '-01-01', sep='')))
}

# d <- as.Date('2018-01-01')
df <- df %>% filter(dateCommissionAC >= ny())


# t <- df %>% group_by(intitule,documentLabel,dateCommissionAC,statutEnquete,periodicite) %>%
            # summarise(n = n()) %>% ungroup() %>%
            # order_by(arrange(.,dateCommissionAC))

t <- df %>% group_by(intitule, documentLabel, statutEnquete, dateCommissionAC, dateDebutAC, dateFinAC,
                     commissionLabel, serviceProducteurPrincipal,
                     servicesProducteurs, periodicite, commissionCNIS) %>% 
            summarise() %>% ungroup() %>%
            order_by(arrange(.,dateCommissionAC))

t <- t %>% mutate(anneeCommission = format(dateCommissionAC, "%_Y"))

write_year <- function(t, years) {
  for (year in years) {
    t %>% filter(dateCommissionAC >= ny(year) & dateCommissionAC < ny(year+1)) %>% 
      aws.s3::s3write_using(
        FUN = write.csv,
        row.names = F,
        object = paste('activite_', as.character(year),'.csv', sep = ''),
        bucket = BUCKET, 
        opts = list("region" = ""))
  }
}

t %>% 
  aws.s3::s3write_using(
    FUN = write.csv,
    row.names = F,
    object = 'activite_comite.csv',
    bucket = BUCKET, 
    opts = list("region" = ""))

write_year(t, 2018:2024)

x <- t %>% group_by(anneeCommission) %>% summarise(nbDossiers = count(documentLabel))
