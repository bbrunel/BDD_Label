library(aws.s3)
library(dplyr)


# Import des données
BUCKET = "bbrunel"

aoc <- aws.s3::s3read_using(
        FUN = read.csv,
        na.strings = c("", "NA"),
        object = "BDD_Label/AC.csv",
        bucket = BUCKET,
        opts = list("region" = ""))
enq <- aws.s3::s3read_using(
        FUN = read.csv,
        na.strings = c("", "NA"),
        object = "BDD_Label/Enquetes.csv",
        bucket = BUCKET,
        opts = list("region" = ""))

# On sépare la table des avis d'opportunité/conformité
ac <- aoc %>% filter(Type.davis == "Avis de conformité")
ao <- aoc %>% filter(Type.davis == "Avis d'opportunité")

# On effectue la jointure des deux tables
data <- enq %>% left_join(y = ac, by = join_by(y$Slug == x$Avis.de.conformité))
data_ao <- enq %>% left_join(y = ao, by = join_by(y$Slug == x$Avis.dopportunité))

# Ajout des date AO# Ajout dTitle.xes date AO
data <- data %>% mutate(dateCommissionAO = data_ao[.$ID.x == data_ao$ID.x,]$Date.commission...formation,
                        dateDebutAO = data_ao[.$ID.x == data_ao$ID.x,]$Date.début.de.validité,
                        dateFinAO = data_ao[.$ID.x == data_ao$ID.x,]$Date.fin.de.validité)

# On conserve uniquement les colonnes d'intérêt, modification noms colonnes

data <- data %>% transmute(parutionJO = Paru.au.Journal.Officiel.du,
                           titre = Title.x, intitule = Intitulé.de.lenquête,
                           statutEnquete = Statut.de.l.enquête,
                           numeroVISA = Numéro.de.VISA,
                           nouvelleEdition = Enquête.nouvelle.édition, initiative = Initiative,
                           contenuQuestionnaire = Contenu.du.questionnaire,
                           uniteStatistique = Unité.statistique.enquêtée,
                           typeEnquete = Type.denquête,
                           champGeographique = Champ.géographique,
                           extensionsGeographiques = Extensions.géographiques,
                           periodicite = Périodicité.de.lenquête,
                           tailleEchantillon = Taille.de.léchantillon, 
                           #modeDeCollecte =  Mode.de.collecte,
                           serviceProducteurPrincipal =  Service.producteur.principal,
                           servicesProducteurs = Services.producteurs,
                           dateCommissionAC = Date.commission...formation,
                           dateDebutAC = Date.début.de.validité, dateFinAC = Date.fin.de.validité, 
                           commissionLabel = Commission.label, documentLabel = Document,
                           dateCommissionAO, dateDebutAO, dateFinAO, commissionCNIS = Commission.CNIS)


# Sauvegarde au format csv
aws.s3::s3write_using(data,
                      FUN = write.csv,
                      row.names = F,
                      object = "BDD_Label/Enquetes_AC.csv",
                      bucket = BUCKET,
                      opts = list("region" = ""))
