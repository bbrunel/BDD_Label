rm(list=ls())

library(aws.s3)
library(dplyr)

BUCKET = "bbrunel"

aoc <- aws.s3::s3read_using(
        FUN = read.csv,
        stringsAsFactors = F,
        na.strings = c("", "NA"),
        object = "BDD_Label/AC.csv",
        bucket = BUCKET,
        opts = list("region" = ""))

aoc_old <- aws.s3::s3read_using(
            FUN = read.csv,
            stringsAsFactors = F,
            na.strings = c("", "NA"),
            object = "BDD_Label/AC_OLD.csv",
            bucket = BUCKET,
            opts = list("region" = ""))

enq <- aws.s3::s3read_using(
        FUN = read.csv,
        stringsAsFactors = T,
        na.strings = c("", "NA"),
        object = "BDD_Label/Enquetes.csv",
        bucket = BUCKET,
        opts = list("region" = ""))

enq_old <- aws.s3::s3read_using(
            FUN = read.csv,
            stringsAsFactors = T,
            na.strings = c("", "NA"),
            object = "BDD_Label/Enquetes_OLD.csv",
            bucket = BUCKET,
            opts = list("region" = ""))

compareNA <- function(v1,v2) {
  same <- (v1 == v2) | (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}


diff_enq <- c()
for (id in enq$ID) {
  if (!all(compareNA(enq[enq$ID == id,], enq_old[enq_old$ID == id,]))) {
    diff_enq <- c(diff_enq, id)
  }
}

diff_aoc <- c()
for (id in aoc$ID) {
  if (!(id %in% aoc_old$ID)) {
    diff_aoc <- c(diff_aoc, id)
  }
  else if (!all(compareNA(aoc[aoc$ID == id,], aoc_old[aoc_old$ID == id,]))) {
    diff_aoc <- c(diff_aoc, id)
  }
}

write.csv(aoc %>% filter(ID %in% diff_aoc), "ac_new.csv")
write.csv(aoc_old %>% filter(ID %in% diff_aoc), "ac_old.csv")
write.csv(enq %>% filter(ID %in% diff_enq), "enq_new.csv")
write.csv(enq_old %>% filter(ID %in% diff_enq), "enq_old.csv")
