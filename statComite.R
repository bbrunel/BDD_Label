library(dplyr)
library(aws.s3)

rm(list = ls())

activite <- aws.s3::s3read_using(FUN = read.csv,
                                 stringsAsFactors = T,
                                 object = "BDD_Label/activite_comite.csv",
                                 bucket = "bbrunel",
                                 opts = list("region" = ""))

# activite %>%  aws.s3::s3write_using(FUN = write.csv,
# row.names = F,
# object = "BDD_Label/activite_comite.csv",
# bucket = "bbrunel",
# opts = list("region" = ""))


x <- activite %>% group_by(dateCommissionAC) %>% summarise(n = sum(chargeTravail)) %>% filter(n > 2)

a2018 <- activite %>% filter(anneeCommission == 2018)

View(a2018 %>% group_by(dateCommissionAC) %>% summarise(n = n()))
t <- activite %>% filter(dateCommissionAC == "2018-05-29")

View(activite %>% filter(dateCommissionAC %in% x$dateCommissionAC))


t <- activite %>% filter(prolongation + rectification == 0) %>% 
  group_by(anneeCommission) %>% 
  summarise(nbDossier = n())

level(activite$statutEnquete)
levels(as.factor(c("lol","cock","lol")))


activite <- activite %>% mutate(obligation = as.factor(case_when(as.integer(statutEnquete) >= 3 ~ "Oui",
                                                       .default = "Non")))

activite <- activite %>% mutate(typeAvis = as.factor(case_match(as.integer(statutEnquete),
                                                      c(1,4) ~ "Examen",
                                                      c(2,3) ~ "Conformité")))
