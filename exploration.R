rm(list=ls())

library(aws.s3)
library(dplyr)

BUCKET <- "bbrunel"

df <- aws.s3::s3read_using(FUN = read.csv,
                           stringsAsFactors = T,
                           na.strings = c("","NA"),
                           object = "BDD_Label/Enquetes_AC.csv",
                           bucket = BUCKET,
                           opts = list("region" = ""))
levels(df$initiative)
df <- df %>% 
      mutate(avisConformite = case_match(statutEnquete,
                                         "Enquête d'intérêt général et de qualité statistique" ~ 1,
                                         "Enquête d'intérêt général et de qualité statistique à caractère obligatoire" ~ 1,
                                         .default = 0))

doc2020 <- df %>% filter(avisConformite == 1 & grepl('2020', dateCommissionAC)) %>% select(documentLabel)
x <- df %>% filter(is.na(commissionLabel))

sum(x$avisConformite)

enq <- enq <- aws.s3::s3read_using(
  FUN = read.csv,
  na.strings = c("", "NA"),
  object = "BDD_Label/Enquetes.csv",
  bucket = BUCKET,
  opts = list("region" = ""))

y <- enq %>% filter(is.na(Avis.de.conformité))

y1 <- y %>% filter(grepl("qualité statistique", Statut.de.l.enquête)) %>% select(ID,Title,Statut.de.l.enquête,Numéro.de.VISA)
write.csv(y1[sample(1:nrow(y1),20),],"example1.csv",row.names=F)

y2 <-  y %>% filter(!grepl("qualité statistique", Statut.de.l.enquête)) %>% select(ID,Title,Statut.de.l.enquête,Numéro.de.VISA)
write.csv(y2[sample(1:nrow(y2),20),],"example2.csv",row.names=F)
