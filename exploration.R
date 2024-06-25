rm(list=ls())

library(aws.s3)
library(dplyr)
library(stringdist)

BUCKET <- "bbrunel"

df <- aws.s3::s3read_using(FUN = read.csv,
                           stringsAsFactors = T,
                           na.strings = c("","NA"),
                           object = "BDD_Label/Enquetes_AC.csv",
                           bucket = BUCKET,
                           opts = list("region" = ""))

levels(df$initiative)
?read.csv
df <- df %>% mutate(dateCommissionAC = as.Date(dateCommissionAC))
df <- df %>% 
      mutate(avisConformite = case_match(statutEnquete,
                                         "Enquête d'intérêt général et de qualité statistique" ~ 1,
                                         "Enquête d'intérêt général et de qualité statistique à caractère obligatoire" ~ 1,
                                         .default = 0))

doc2020 <- df %>% filter(avisConformite == 1 & grepl('2020', dateCommissionAC)) %>% select(documentLabel)
x <- df %>% filter(is.na(commissionLabel))

sum(x$avisConformite)

enq <- aws.s3::s3read_using(
  FUN = read.csv,
  na.strings = c("", "NA"),
  object = "BDD_Label/Enquetes.csv",
  bucket = BUCKET,
  opts = list("region" = ""))

y <- enq %>% filter(is.na(Avis.de.conformité))

y1 <- y %>% filter(grepl("qualité statistique", Statut.de.l.enquête)) %>% select(ID,Title,Statut.de.l.enquête,Numéro.de.VISA,Services.producteurs)
write.csv(y1,"example1.csv",row.names=F)

y2 <-  y %>% filter(!grepl("qualité statistique", Statut.de.l.enquête)) %>% select(ID,Intitulé.de.lenquête,Statut.de.l.enquête,Numéro.de.VISA,Services.producteurs)
write.csv(y2,"example2.csv",row.names=F)
nrow(y2[is.na(y2$Statut.de.l.enquête),])


ac <- aws.s3::s3read_using(
  FUN = read.csv,
  na.strings = c("", "NA"),
  object = "BDD_Label/AC.csv",
  bucket = BUCKET,
  opts = list("region" = ""))
ac <- ac %>% filter(Type.davis == "Avis de conformité")

ac %>% filter(grepl("branche.+administration", Title)) %>% select(Title)
ac %>% filter(grepl("Emploi",Title)) %>% select(Title)

f <- function(enq) {
  enq <- y1
  ids <- c()
  d <- c()
  for (i in enq$ID) {
    e <- enq[enq$ID == i,]
    ac$dist <- stringdist(e$Title,ac$Title,method="lv")
    x <- sort_by(ac,ac$dist)
    if (is.na(e$Numéro.de.VISA)) {
      ids <- c(ids, x[1,]$ID)
      d <- c(d,ac[ac$ID == x[1,]$ID,]$dist)
      next
    }
    date <- as.numeric(substr(e$Numéro.de.VISA, 1, 4))
    db <- as.numeric(substr(x$Date.début.de.validité, 1, 4))
    df <- as.numeric(substr(x$Date.fin.de.validité, 1, 4))
    b <- date >= db & date <= df
    if (length(b) == 0) {
      ids <- c(ids, NA)
      d <- c(d,NA)
    }
    else {
      ids <- c(ids, x[b,]$ID[1])
      d <- c(d, ac[ac$ID == x[b,]$ID[1],]$dist)
    }

  }
  return(list(ids,d))
}
rf <- f(y1)
y1 <- y1 %>% mutate(id_ac = rf[[1]], dist = rf[[2]])
df <- y1 %>%  inner_join(y = (ac %>% select(ID,Title,Document,Date.début.de.validité,Date.fin.de.validité)), by = join_by(x$id_ac == y$ID))
