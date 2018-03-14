###################################################################
################# Proportional Closed Races #######################
###################################################################

# This script clean, combine and rank data of Brazilian candidates in proportional elections. 
# The project aims to analyze the proportional races and to generate datasets that 
# have vote shares using different metrics


######## Summary ########

# 1. Downloading, organizing and cleaning data about votes and candidates
# 2. Cleaning errors in candidates datasets
# 3. Generating Datasets with votes and candidates
# 4. Downloading, organizing and cleaning data about partisan votes
# 5. Preliminary tests
# 6. Functions to generate RDD Datasets



###################################################################
###################################################################
###### 1.Downloading, Organizing and Cleaning Electoral Data ######
#0. Download TSE repositorio data
#1. Combine 
#2. Clean and get it ready for use
###################################################################
###################################################################

#Preambule
rm(list=ls())

options(scipen=999) # supressing scientific notation
par(mar=c(5.1,4.1,4.1,2.1)) 
par(mfrow=c(1,1))

#libraries used
library(tidyverse)
library(eeptools)
library(readr)
library(qpcR)
library(readxl)

#Change your working directory here
setwd("~")
if(grepl("nataliabueno",getwd())==TRUE){#Allow for different paths in our computers
  dir <- "~/Dropbox/LOCAL_ELECTIONS/"
}else{
  dir <- "//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/"
}

#helper functions
source(paste0(dir, "codes/helper_functions.R"))

###################################################################
#0. Downloading 
###################################################################

dir_d <- "//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/"

#Candidate data
url_cand98 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_1998.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_1998.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_1998")
cand_1998 <- get_tse(url_cand98, file_d, file_un)

url_cand00 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2000.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2000.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2000")
cand_2000 <- get_tse(url_cand00, file_d, file_un)

url_cand02 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2002.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2002.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2002")
cand_2002 <- get_tse(url_cand02, file_d, file_un)

url_cand04 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2004.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2004.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2004")
cand_2004 <- get_tse(url_cand04, file_d, file_un)

url_cand06 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2006.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2006.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2006")
cand_2006 <- get_tse(url_cand06, file_d, file_un)

url_cand08 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2008.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2008.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2008")
cand_2008 <- get_tse(url_cand08, file_d, file_un)

url_cand10 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2010.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2010.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2010")
cand_2010 <- get_tse(url_cand10, file_d, file_un)

url_cand12 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2012.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2012.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2012")
cand_2012 <- get_tse(url_cand12, file_d, file_un)

url_cand14 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2014.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2014.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2014")
cand_2014 <- get_tse(url_cand14, file_d, file_un)

url_cand16 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2016.zip"
file_d <- paste0(dir_d, "original_data/consulta_cand/consulta_cand_2016.zip")
file_un <- paste0(dir_d, "original_unzipped/consulta_cand/consulta_cand_2016")
cand_2016 <- get_tse(url_cand16, file_d, file_un)

#Voting data
url_vot98 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_1998.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_1998.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_1998")
vot_1998 <- get_tse(url_vot98, file_d, file_un)

url_vot00 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2000.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2000.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2000")
vot_2000 <- get_tse(url_vot00, file_d, file_un)

url_vot02 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2002.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2002.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2002")
vot_2002 <- get_tse(url_vot02, file_d, file_un)

url_vot04 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2004.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2004.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2004")
vot_2004 <- get_tse(url_vot04, file_d, file_un)

url_vot06 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2006.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2006.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2006")
vot_2006 <- get_tse(url_vot06, file_d, file_un)

url_vot08 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2008.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2008.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2008")
vot_2008 <- get_tse(url_vot08, file_d, file_un)

url_vot10 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2010.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2010.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2010")
vot_2010 <- get_tse(url_vot10, file_d, file_un)

url_vot12 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2012.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2012.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2012")
vot_2012 <- get_tse(url_vot12, file_d, file_un)

url_vot14 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2014.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2014.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2014")
vot_2014 <- get_tse(url_vot14, file_d, file_un)

url_vot16 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2016.zip"
file_d <- paste0(dir_d, "original_data/votacao_munzona/votacao_candidato_munzona_2016.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_munzona/votacao_candidato_munzona_2016")
vot_2016 <- get_tse(url_vot16, file_d, file_un)

###################################################################
#1. Combining

### LOCAL ELECTIONS

# Reading candidate data
# Reading voting data
# Binding

### NATIONAL ELECTIONS

# Reading candidate data
# Reading voting data
# Binding

###################################################################

ufs_n <- c("AC", "AL", "AP", "AM", "BA", "BR",   
           "CE", "DF", "ES", "GO", "MA", "MT", "MS",
           "MG", "PA", "PB", "PR", "PE", "PI", "RJ",
           "RN", "RS", "RO","RR","SC", "SP", "SE", "TO", "ZZ")

ufs <- c("AC", "AL", "AP", "AM", "BA", "BR",   
           "CE", "DF", "ES", "GO", "MA", "MT", "MS",
           "MG", "PA", "PB", "PR", "PE", "PI", "RJ",
           "RN", "RS", "RO","RR","SC", "SP", "SE", "TO", "ZZ")

labels_pre2012c <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                     "SIGLA_UF", "SIGLA_UE", "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO",
                     "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", "CPF_CANDIDATO",
                     "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", "DES_SITUACAO_CANDIDATURA",
                     "NUMERO_PARTIDO", "SIGLA_PARTIDO", "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA",
                     "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", "DESCRICAO_OCUPACAO",
                     "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", "IDADE_DATA_ELEICAO",
                     "CODIGO_SEXO", "DESCRICAO_SEXO", "COD_GRAU_INSTRUCAO", "DESCRICAO_GRAU_INSTRUCAO",
                     "CODIGO_ESTADO_CIVIL", "DESCRICAO_ESTADO_CIVIL", "CODIGO_NACIONALIDADE",
                     "DESCRICAO_NACIONALIDADE", "SIGLA_UF_NASCIMENTO", "CODIGO_MUNICIPIO_NASCIMENTO",
                     "NOME_MUNICIPIO_NASCIMENTO", "DESPESA_MAX_CAMPANHA", "COD_SIT_TOT_TURNO",
                     "DESC_SIT_TOT_TURNO")

#candidates 2000
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2000/consulta_cand_2000_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
cand_2000 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2000 <- do.call("rbind", cand_2000)
names(cand_2000) <- labels_pre2012c
cand_2000 <- as_tibble(cand_2000)

#candidates 2004
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2004/consulta_cand_2004_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
cand_2004 <- lapply(files, read.table, sep = ";", header=F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2004 <- do.call("rbind", cand_2004)
names(cand_2004) <- labels_pre2012c
cand_2004 <- as_tibble(cand_2004)

#candidates 2008
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2008/consulta_cand_2008_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
cand_2008 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2008 <- do.call("rbind", cand_2008)
names(cand_2008) <- labels_pre2012c
cand_2008 <- as_tibble(cand_2008)

#candidates 2012
labels_2012c <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                  "SIGLA_UF", "SIGLA_UE", "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO",
                  "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", "CPF_CANDIDATO",
                  "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", "DES_SITUACAO_CANDIDATURA",
                  "NUMERO_PARTIDO", "SIGLA_PARTIDO", "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA",
                  "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", "DESCRICAO_OCUPACAO",
                  "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", "IDADE_DATA_ELEICAO",
                  "CODIGO_SEXO", "DESCRICAO_SEXO", "COD_GRAU_INSTRUCAO", "DESCRICAO_GRAU_INSTRUCAO",
                  "CODIGO_ESTADO_CIVIL", "DESCRICAO_ESTADO_CIVIL", "CODIGO_NACIONALIDADE",
                  "DESCRICAO_NACIONALIDADE", "SIGLA_UF_NASCIMENTO", "CODIGO_MUNICIPIO_NASCIMENTO",
                  "NOME_MUNICIPIO_NASCIMENTO", "DESPESA_MAX_CAMPANHA", "COD_SIT_TOT_TURNO",
                  "DESC_SIT_TOT_TURNO", "EMAIL_CANDIDATO")

files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2012/consulta_cand_2012_", 
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
cand_2012 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2012 <- do.call("rbind", cand_2012)
names(cand_2012) <- labels_2012c
cand_2012 <- as_tibble(cand_2012)

#candidates 2016
labels_2016c <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                  "SIGLA_UF", "SIGLA_UE", "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO",
                  "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", "CPF_CANDIDATO",
                  "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", "DES_SITUACAO_CANDIDATURA",
                  "NUMERO_PARTIDO", "SIGLA_PARTIDO", "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA",
                  "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", "DESCRICAO_OCUPACAO",
                  "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", "IDADE_DATA_ELEICAO",
                  "CODIGO_SEXO", "DESCRICAO_SEXO", "COD_GRAU_INSTRUCAO", "DESCRICAO_GRAU_INSTRUCAO",
                  "CODIGO_ESTADO_CIVIL", "DESCRICAO_ESTADO_CIVIL", "CODIGO_COR_RACA", "DESCRICAO_COR_RACA",
                  "CODIGO_NACIONALIDADE", "DESCRICAO_NACIONALIDADE", "SIGLA_UF_NASCIMENTO", 
                  "CODIGO_MUNICIPIO_NASCIMENTO", "NOME_MUNICIPIO_NASCIMENTO", "DESPESA_MAX_CAMPANHA",
                  "COD_SIT_TOT_TURNO", "DESC_SIT_TOT_TURNO", "EMAIL_CANDIDATO")

files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2016/consulta_cand_2016_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
cand_2016 <- lapply(files, read.table, sep = ";", 
                    header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2016 <- do.call("rbind", cand_2016)
names(cand_2016) <- labels_2016c
cand_2016 <- as_tibble(cand_2016)

cand_2000_2016 <- list(cand_2000, cand_2004, cand_2008, cand_2012, cand_2016)
save(cand_2000_2016, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_2000_2016.RData")

#Voting data 2000
labels_pre2012 <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                    "SIGLA_UF", "SIGLA_UE", "CODIGO_MUNICIPIO", "NOME_MUNICIPIO", "NUMERO_ZONA",
                    "CODIGO_CARGO", "NUMERO_CAND", "SQ_CANDIDATO", "NOME_CANDIDATO", "NOME_URNA_CANDIDATO",
                    "DESCRICAO_CARGO", "COD_SIT_CAND_SUPERIOR", "DESC_SIT_CAND_SUPERIOR", "CODIGO_SIT_CANDIDATO",
                    "DESC_SIT_CANDIDATO", "CODIGO_SIT_CAND_TOT", "DESC_SIT_CAND_TOT", "NUMERO_PARTIDO",
                    "SIGLA_PARTIDO", "NOME_PARTIDO", "SEQUENCIAL_LEGENDA", "NOME_COLIGACAO", "COMPOSICAO_LEGENDA",
                    "TOTAL_VOTOS")

#Voting 2000
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2000/votacao_candidato_munzona_2000_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
vot_2000 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2000 <- do.call("rbind", vot_2000)
names(vot_2000) <- labels_pre2012
vot_2000 <- as_tibble(vot_2000)

#Voting data 2004
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2004/votacao_candidato_munzona_2004_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
vot_2004 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2004 <- do.call("rbind", vot_2004)
names(vot_2004) <- labels_pre2012
vot_2004 <- as_tibble(vot_2004)

#Voting data 2008
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2008/votacao_candidato_munzona_2008_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
vot_2008 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors=F, fill = T, fileEncoding = "windows-1252") 
vot_2008 <- do.call("rbind", vot_2008)
names(vot_2008) <- labels_pre2012
vot_2008 <- as_tibble(vot_2008)

#voting data 2012
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2012/votacao_candidato_munzona_2012_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
vot_2012 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2012 <- do.call("rbind", vot_2012)
names(vot_2012) <- labels_pre2012
vot_2012 <- as_tibble(vot_2012)

labels_2016 <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                 "SIGLA_UF", "SIGLA_UE", "CODIGO_MUNICIPIO", "NOME_MUNICIPIO", "NUMERO_ZONA",
                 "CODIGO_CARGO", "NUMERO_CAND", "SQ_CANDIDATO", "NOME_CANDIDATO", "NOME_URNA_CANDIDATO",
                 "DESCRICAO_CARGO", "COD_SIT_CAND_SUPERIOR", "DESC_SIT_CAND_SUPERIOR", "CODIGO_SIT_CANDIDATO",
                 "DESC_SIT_CANDIDATO", "CODIGO_SIT_CAND_TOT", "DESC_SIT_CAND_TOT", "NUMERO_PARTIDO",
                 "SIGLA_PARTIDO", "NOME_PARTIDO", "SEQUENCIAL_LEGENDA", "NOME_COLIGACAO", "COMPOSICAO_LEGENDA",
                 "TOTAL_VOTOS", "TRANSITO")

#voting data 2016
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2016/votacao_candidato_munzona_2016_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
vot_2016 <- lapply(files, read.table, sep=";", 
                   header=F, stringsAsFactors=F, fill = T, fileEncoding = "latin1") 
vot_2016 <- do.call("rbind", vot_2016)
names(vot_2016) <- labels_2016
vot_2016 <- as_tibble(vot_2016)

vot_2000_2016 <- list(vot_2000, vot_2004, vot_2008, vot_2012, vot_2016)
save(vot_2000_2016, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/vot_2000_2016.RData")

#NATIONAL ELECTIONS

#candidates 1998
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_1998/consulta_cand_1998_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
cand_1998 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_1998 <- do.call("rbind", cand_1998)
names(cand_1998) <- labels_pre2012c
cand_1998 <- as_tibble(cand_1998)

#candidates 2002
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2002/consulta_cand_2002_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
cand_2002 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2002 <- do.call("rbind", cand_2002)
names(cand_2002) <- labels_pre2012c
cand_2002 <- as_tibble(cand_2002)

#candidates 2006
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2006/consulta_cand_2006_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
cand_2006 <- lapply(files, read.table, sep = ";", header=F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2006 <- do.call("rbind", cand_2006)
names(cand_2006) <- labels_pre2012c
cand_2006 <- as_tibble(cand_2006)

#candidates 2010
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2010/consulta_cand_2010_", 
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
cand_2010 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2010 <- do.call("rbind", cand_2010)
names(cand_2010) <- labels_pre2012c
cand_2010 <- as_tibble(cand_2010)

#candidates 2014
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/consulta_cand/consulta_cand_2014/consulta_cand_2014_", 
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
cand_2014 <- lapply(files, read.table, sep = ";", header = F, 
                    stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
cand_2014 <- do.call("rbind", cand_2014)
names(cand_2014) <- labels_2016c
cand_2014 <- as_tibble(cand_2014)

cand_1998_2014 <- list(cand_1998, cand_2002, cand_2006, cand_2010, cand_2014)
save(cand_1998_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_1998_2014.RData")

#Voting data 2000
labels_pre2012 <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                    "SIGLA_UF", "SIGLA_UE", "CODIGO_MUNICIPIO", "NOME_MUNICIPIO", "NUMERO_ZONA",
                    "CODIGO_CARGO", "NUMERO_CAND", "SQ_CANDIDATO", "NOME_CANDIDATO", "NOME_URNA_CANDIDATO",
                    "DESCRICAO_CARGO", "COD_SIT_CAND_SUPERIOR", "DESC_SIT_CAND_SUPERIOR", "CODIGO_SIT_CANDIDATO",
                    "DESC_SIT_CANDIDATO", "CODIGO_SIT_CAND_TOT", "DESC_SIT_CAND_TOT", "NUMERO_PARTIDO",
                    "SIGLA_PARTIDO", "NOME_PARTIDO", "SEQUENCIAL_LEGENDA", "NOME_COLIGACAO", "COMPOSICAO_LEGENDA",
                    "TOTAL_VOTOS")

#Voting 1998
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_1998/votacao_candidato_munzona_1998_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
vot_1998 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_1998 <- do.call("rbind", vot_1998)
names(vot_1998) <- labels_pre2012
vot_1998 <- as_tibble(vot_1998)

#Voting 2002
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2002/votacao_candidato_munzona_2002_",
                        ufs, ".txt"))
vot_2002 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2002 <- do.call("rbind", vot_2002)
names(vot_2002) <- labels_pre2012
vot_2002 <- as_tibble(vot_2002)

#Voting data 2006
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2006/votacao_candidato_munzona_2006_",
                        ufs, ".txt"))
vot_2006 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2006 <- do.call("rbind", vot_2006)
names(vot_2006) <- labels_pre2012
vot_2006 <- as_tibble(vot_2006)

#Voting data 2010
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2010/votacao_candidato_munzona_2010_",
                        ufs, ".txt"))
vot_2010 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors=F, fill = T, fileEncoding = "windows-1252") 
vot_2010 <- do.call("rbind", vot_2010)
names(vot_2010) <- labels_pre2012
vot_2010 <- as_tibble(vot_2010)

#voting data 2014
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_munzona/votacao_candidato_munzona_2014/votacao_candidato_munzona_2014_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
vot_2014 <- lapply(files, read.table, sep = ";", 
                   header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
vot_2014 <- do.call("rbind", vot_2014)
names(vot_2014) <- labels_2016
vot_2014 <- as_tibble(vot_2014)

vot_1998_2014 <- list(vot_1998, vot_2002, vot_2006, vot_2010, vot_2014)
save(vot_1998_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/vot_1998_2014.RData")


###################################################################
###################################################################
######       2.Cleaning errors in candidates datasets       ######
#0. Loading data
#1. Identifying and cleaning errors
###################################################################
###################################################################

#### 0. Loading data
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_2000_2016.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_1998_2014.RData")

cand_1998 <- cand_1998_2014[[1]]
cand_2000 <- cand_2000_2016[[1]]
cand_2002 <- cand_1998_2014[[2]]
cand_2004 <- cand_2000_2016[[2]]
cand_2006 <- cand_1998_2014[[3]]
cand_2008 <- cand_2000_2016[[3]]
cand_2010 <- cand_1998_2014[[4]]
cand_2012 <- cand_2000_2016[[4]]
cand_2014 <- cand_1998_2014[[5]]
cand_2016 <- cand_2000_2016[[5]]


#### 1. Identifying and cleaning errors
#1998
cand_1998 <- mutate(cand_1998, id=rownames(cand_1998))
problems <- cand_1998 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_1998 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

wrong_ballot <- casos %>% right_join(problems_caso, by = c("NUM_TURNO", 
                                                           "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#Exclude
wrong_ballot <- wrong_ballot %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                     NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                     NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, id)) 

write.csv(wrong_ballot, file = paste0(dir, "cepesp_data/wrong_ballot.csv"))

#WHAT TO EXCLUDE
#cand_1998 <- cand_1998_2014[[1]]
#cand_1998 <- mutate(cand_1998, id=rownames(cand_1998))
problems <- cand_1998 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_1998 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))


#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4))
casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, id)) #save

case_key <-  read_excel(paste0(dir, "cepesp_data/wrong_ballot1998_excl_Key.xlsx"))

exclude <- c( case_key$key, casos_notvalid$key)

cand_1998 <- cand_1998 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO,id))

cand_1998v2 <- cand_1998 %>% filter(!(key %in% exclude))
cand_1998v2 <- cand_1998v2[,-ncol(cand_1998v2)]
cand_1998v2$id<-NULL

problems2 <- cand_1998v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos2 <- cand_1998v2 %>% right_join(problems2, by = c("NUM_TURNO", 
                                                       "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))
repeated_casos2 <- casos2[duplicated(casos2),] #save

repeated_casos2 <- repeated_casos2 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                           NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                           NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO))

exclude2 <- c( repeated_casos2$key)

cand_1998v2 <- cand_1998v2 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                   NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                   NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO))

cand_1998v3 <- cand_1998v2 %>% filter(!(key %in% exclude2))
cand_1998v3 <- cand_1998v3[,-ncol(cand_1998v3)]


save(cand_1998v3, file = paste0(dir, "cepesp_data/cand_1998v3.Rda"))
write.csv2(cand_1998v3, file = paste0(dir, "cepesp_data/cand_1998v3.csv"), row.names=FALSE, fileEncoding = "UTF-8")

#Smell test
temp <- cand_1998 %>% filter(key %in% exclude)
stopifnot((nrow(cand_1998) - nrow(cand_1998v2)) == nrow(temp))

#2000

cand_2000v1 <- cand_2000 %>% filter(!(CODIGO_CARGO == 13 & NUM_TURNO == 2))

problems <- cand_2000v1 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                     DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2000v1 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE",
                                                     "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 1 |
                            COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

wrong_ballot <- casos %>% right_join(problems_caso, by = c("NUM_TURNO", 
                                                           "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                           "DESCRICAO_ELEICAO"))

#Exclude
wrong_ballot <- wrong_ballot %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                     NUMERO_CANDIDATO)) 

write.csv(wrong_ballot, file = paste0(dir, "cepesp_data/wrong_ballot_2000.csv"), row.names=FALSE ,sep=";", fileEncoding = "UTF-8")

#2002
problems <- cand_2002 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2002 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2002 <- cand_1998_2014[[2]]
problems <- cand_2002 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2002 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4))
casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2002 <- cand_2002 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2002v2 <- cand_2002 %>% filter(!(key %in% exclude))
cand_2002v2 <- cand_2002v2[,-ncol(cand_2002v2)]

save(cand_2002v2, file = paste0(dir, "cepesp_data/cand_2002v2.Rda"))
write.csv2(cand_2002v2, file = paste0(dir, "cepesp_data/cand_2002v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2002 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2002) - nrow(cand_2002v2)) == nrow(temp))

#2004
problems <- cand_2004 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2004 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)


#WHAT TO EXCLUDE
cand_2004 <- cand_2000_2016[[2]]
problems <- cand_2004 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2004 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4))
casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2004 <- cand_2004 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2004v2 <- cand_2004 %>% filter(!(key %in% exclude))
cand_2004v2 <- cand_2004v2[,-ncol(cand_2004v2)]

save(cand_2004v2, file = paste0(dir, "cepesp_data/cand_2004v2.Rda"))
write.csv2(cand_2004v2, file = paste0(dir, "cepesp_data/cand_2004v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2004 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2004) - nrow(cand_2004v2)) == nrow(temp))

#2006
problems <- cand_2006 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2006 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 | COD_SITUACAO_CANDIDATURA == 16)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2006 <- cand_1998_2014[[3]]
problems <- cand_2006 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2006 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 | COD_SITUACAO_CANDIDATURA == 16))
casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2006 <- cand_2006 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2006v2 <- cand_2006 %>% filter(!(key %in% exclude))
cand_2006v2 <- cand_2006v2[,-ncol(cand_2006v2)]

save(cand_2006v2, file = paste0(dir, "cepesp_data/cand_2006v2.Rda"))
write.csv2(cand_2006v2, file = paste0(dir, "cepesp_data/cand_2006v2.csv"), row.names=FALSE, fileEncoding = "UTF-8")

#Smell test
temp <- cand_2006 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2006) - nrow(cand_2006v2)) == nrow(temp))

#2008
problems <- cand_2008 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2008 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 |
                            COD_SITUACAO_CANDIDATURA == 8 |
                            COD_SITUACAO_CANDIDATURA == 16 | 
                            COD_SITUACAO_CANDIDATURA == 17)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                    DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2008 <- cand_2000_2016[[3]]
problems <- cand_2008 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2008 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 |
                                       COD_SITUACAO_CANDIDATURA == 8 |
                                       COD_SITUACAO_CANDIDATURA == 16 | 
                                       COD_SITUACAO_CANDIDATURA == 17))


casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2008 <- cand_2008 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2008v2 <- cand_2008 %>% filter(!(key %in% exclude))
cand_2008v2 <- cand_2008v2[,-ncol(cand_2008v2)]

save(cand_2008v2, file = paste0(dir, "cepesp_data/cand_2008v2.Rda"))
write.csv2(cand_2008v2, file = paste0(dir, "cepesp_data/cand_2008v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2008 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2008) - nrow(cand_2008v2)) == nrow(temp))

#2010
problems <- cand_2010 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2010 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 |
                            COD_SITUACAO_CANDIDATURA == 16 | 
                            COD_SITUACAO_CANDIDATURA == 17 |
                            COD_SITUACAO_CANDIDATURA == 18)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                    DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2010 <- cand_1998_2014[[4]]
problems <- cand_2010 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2010 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 |
                                       COD_SITUACAO_CANDIDATURA == 16 | 
                                       COD_SITUACAO_CANDIDATURA == 17 |
                                       COD_SITUACAO_CANDIDATURA == 18))

casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, SEQUENCIAL_CANDIDATO,
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2010 <- cand_2010 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO,SEQUENCIAL_CANDIDATO,
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2010v2 <- cand_2010 %>% filter(!(key %in% exclude))
cand_2010v2 <- cand_2010v2[,-ncol(cand_2010v2)]

save(cand_2010v2, file = paste0(dir, "cepesp_data/cand_2010v2.Rda"))
write.csv2(cand_2010v2, file = paste0(dir, "cepesp_data/cand_2010v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2010 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2010) - nrow(cand_2010v2)) == nrow(temp))

#2012
problems <- cand_2012 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2012 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 |
                            COD_SITUACAO_CANDIDATURA == 8 |
                            COD_SITUACAO_CANDIDATURA == 16 | 
                            COD_SITUACAO_CANDIDATURA == 17 |
                            COD_SITUACAO_CANDIDATURA == 18)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                    DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2012 <- cand_2000_2016[[4]]
problems <- cand_2012 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2012 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO,
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 |
                                       COD_SITUACAO_CANDIDATURA == 8 |
                                       COD_SITUACAO_CANDIDATURA == 16 | 
                                       COD_SITUACAO_CANDIDATURA == 17 |
                                       COD_SITUACAO_CANDIDATURA == 18))

casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2012 <- cand_2012 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2012v2 <- cand_2012 %>% filter(!(key %in% exclude))
cand_2012v2 <- cand_2012v2[,-ncol(cand_2012v2)]

save(cand_2012v2, file = paste0(dir, "cepesp_data/cand_2012v2.Rda"))
write.csv2(cand_2012v2, file = paste0(dir, "cepesp_data/cand_2012v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2012 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2012) - nrow(cand_2012v2)) == nrow(temp))

#2014
problems <- cand_2014 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2014 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 |
                            COD_SITUACAO_CANDIDATURA == 16 | 
                            COD_SITUACAO_CANDIDATURA == 17)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                    DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2014 <- cand_1998_2014[[5]]
problems <- cand_2014 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2014 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 |
                                       COD_SITUACAO_CANDIDATURA == 16 | 
                                       COD_SITUACAO_CANDIDATURA == 17))

casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2014 <- cand_2014 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2014v2 <- cand_2014 %>% filter(!(key %in% exclude))
cand_2014v2 <- cand_2014v2[,-ncol(cand_2014v2)]

save(cand_2014v2, file = paste0(dir, "cepesp_data/cand_2014v2.Rda"))
write.csv2(cand_2014v2, file = paste0(dir, "cepesp_data/cand_2014v2.csv"), row.names=FALSE , fileEncoding = "UTF-8")

#Smell test
temp <- cand_2014 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2014) - nrow(cand_2014v2)) == nrow(temp))

#2016
problems <- cand_2016 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2016 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

#Control for repeated
repeated_casos <- casos[duplicated(casos),]

#But keeping only unique
casos <- unique(casos)

casos <- casos %>% filter(COD_SITUACAO_CANDIDATURA == 2 | 
                            COD_SITUACAO_CANDIDATURA == 4 |
                            COD_SITUACAO_CANDIDATURA == 8 |
                            COD_SITUACAO_CANDIDATURA == 16 | 
                            COD_SITUACAO_CANDIDATURA == 17 |
                            COD_SITUACAO_CANDIDATURA == 18 |
                            COD_SITUACAO_CANDIDATURA == 19)

problems_caso <- casos %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                    DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

#WHAT TO EXCLUDE
cand_2016 <- cand_2000_2016[[5]]
problems <- cand_2016 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE, 
                                   DESCRICAO_ELEICAO) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2016 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE", 
                                                   "DESCRICAO_ELEICAO"))

repeated_casos <- casos[duplicated(casos),] #save
repeated_casos <- repeated_casos %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

#But keeping only unique
casos <- unique(casos)
casos_notvalid <- casos %>% filter(!(COD_SITUACAO_CANDIDATURA == 2 | 
                                       COD_SITUACAO_CANDIDATURA == 4 |
                                       COD_SITUACAO_CANDIDATURA == 8 |
                                       COD_SITUACAO_CANDIDATURA == 16 | 
                                       COD_SITUACAO_CANDIDATURA == 17 |
                                       COD_SITUACAO_CANDIDATURA == 18 |
                                       COD_SITUACAO_CANDIDATURA == 19))

casos_notvalid <- casos_notvalid %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                                         NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                                         NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO)) #save

exclude <- c(repeated_casos$key, casos_notvalid$key)

cand_2016 <- cand_2016 %>% mutate(key = paste0(SIGLA_UE, NUM_TURNO, NOME_URNA_CANDIDATO, 
                                               NUMERO_CANDIDATO, DESCRICAO_ELEICAO, 
                                               NUM_TITULO_ELEITORAL_CANDIDATO, CODIGO_CARGO, SEQUENCIAL_CANDIDATO))

cand_2016v2 <- cand_2016 %>% filter(!(key %in% exclude))
cand_2016v2 <- cand_2016v2[,-ncol(cand_2016v2)]

save(cand_2016v2, file = paste0(dir, "cepesp_data/cand_2016v2.Rda"))
write.csv2(cand_2016v2, file = paste0(dir, "cepesp_data/cand_2016v2.csv"), row.names=FALSE, fileEncoding = "UTF-8")

#Smell test
temp <- cand_2016 %>% filter(key %in% exclude)
stopifnot((nrow(cand_2016) - nrow(cand_2016v2)) == nrow(temp))


#consolidating

cand_2000_2016v2 <- list(cand_2000, cand_2004v2, cand_2008v2, cand_2012v2, cand_2016v2)
save(cand_2000_2016v2, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_2000_2016v2.RData")

cand_1998_2014v2 <- list(cand_1998v3, cand_2002v2, cand_2006v2, cand_2010v2, cand_2014v2)
save(cand_1998_2014v2, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_1998_2014v2.RData")




###################################################################
###################################################################
######   1.Generating Datasets with votes and candidates     ######
#1. General Elections 
#2. City Council Elections
###################################################################
###################################################################

################# GENERAL ELECTIONS ###################

#loading data
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/vot_1998_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_1998_2014v2.RData")

#######Elections 2002 ###########


library(dplyr)

vot_2002 <- vot_1998_2014[[2]]
cand_2002 <- cand_1998_2014v2[[2]]

vot_2002<- vot_2002 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_02 <- vot_2002 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UF,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging votes with candidates
cand_2002v2 <- cand_2002 %>% left_join(cand_voto_02, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2002, cand_voto_02, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2002v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2002v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                   "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

#### FEDERAL DEPUTY ####

#filter to only Federal representatives

fed_dep_2002<- cand_2002v2 %>%
  filter(CODIGO_CARGO==6)


##ordering electoral coalitions

#creating new sequential
fed_dep_2002 <- mutate(fed_dep_2002, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

#creating id for electoral coalition
fed_dep_2002$idleg <-paste0(fed_dep_2002$SIGLA_UF,fed_dep_2002$CODIGO_CARGO, fed_dep_2002$sq_legenda2)

#ranking candidates
fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

#identifying elected candidates
fed_dep_2002 <-mutate(fed_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

fed_dep_2002$idleg2 <-paste0(fed_dep_2002$idleg, fed_dep_2002$resultado2)

fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

#identifying first suplente
fed_dep_2002 <-mutate(fed_dep_2002, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

#identifying last elected
fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

#selecting valid columns
fed_dep_2002<-fed_dep_2002%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
         CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
         NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
         NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
         DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
         DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
         NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

fed_dep_2002<-fed_dep_2002%>%
  filter(!(is.na(VOTOS)))

#### STATE DEPUTY ####

#filter to only Federal representatives

state_dep_2002<- cand_2002v2 %>%
  filter(CODIGO_CARGO==7)


##ordering electoral coalitions

#creating new sequential
state_dep_2002 <- mutate(state_dep_2002, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

#creating id for electoral coalition
state_dep_2002$idleg <-paste0(state_dep_2002$SIGLA_UF,state_dep_2002$CODIGO_CARGO, state_dep_2002$sq_legenda2)

#ranking candidates
state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

#identifying elected candidates
state_dep_2002 <-mutate(state_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

state_dep_2002$idleg2 <-paste0(state_dep_2002$idleg, state_dep_2002$resultado2)

state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

#identifying first suplente
state_dep_2002 <-mutate(state_dep_2002, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

#identifying last elected
state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

state_dep_2002<-state_dep_2002%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

state_dep_2002<-state_dep_2002%>%
  filter(!(is.na(VOTOS)))
 

#### DISTRITAL DEPUTY ####

#filter to only distrital representatives

distrital_dep_2002<- cand_2002v2 %>%
  filter(CODIGO_CARGO==8)


#ordering electoral coalitions

distrital_dep_2002 <- mutate(distrital_dep_2002, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

distrital_dep_2002$idleg <-paste0(distrital_dep_2002$SIGLA_UF,distrital_dep_2002$CODIGO_CARGO, distrital_dep_2002$sq_legenda2)

distrital_dep_2002 <- distrital_dep_2002 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

distrital_dep_2002 <-mutate(distrital_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

distrital_dep_2002$idleg2 <-paste0(distrital_dep_2002$idleg, distrital_dep_2002$resultado2)

distrital_dep_2002 <- distrital_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

distrital_dep_2002 <-mutate(distrital_dep_2002, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

distrital_dep_2002 <- distrital_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))


distrital_dep_2002<-distrital_dep_2002%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

distrital_dep_2002<-distrital_dep_2002%>%
  filter(!(is.na(VOTOS)))


#######Elections 2006 ###########


library(dplyr)

vot_2006 <- vot_1998_2014[[3]]
cand_2006 <- cand_1998_2014v2[[3]]

vot_2006<- vot_2006 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_06 <- vot_2006 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UF,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2006v2 <- cand_2006 %>% left_join(cand_voto_06, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2006, cand_voto_06, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2006v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2006v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))


#### FEDERAL DEPUTY ####

#filter to only Federal representatives

fed_dep_2006<- cand_2006v2 %>%
  filter(CODIGO_CARGO==6)


#ordering electoral coalitions

fed_dep_2006 <- mutate(fed_dep_2006, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

fed_dep_2006$idleg <-paste0(fed_dep_2006$SIGLA_UF,fed_dep_2006$CODIGO_CARGO, fed_dep_2006$sq_legenda2)

fed_dep_2006 <- fed_dep_2006 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

fed_dep_2006 <-mutate(fed_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

fed_dep_2006$idleg2 <-paste0(fed_dep_2006$idleg, fed_dep_2006$resultado2)

fed_dep_2006 <- fed_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

fed_dep_2006 <-mutate(fed_dep_2006, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

fed_dep_2006 <- fed_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

fed_dep_2006<-fed_dep_2006%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

fed_dep_2006<-fed_dep_2006%>%
  filter(!(is.na(VOTOS)))


#### STATE DEPUTY ####

#filter to only Federal representatives

state_dep_2006<- cand_2006v2 %>%
  filter(CODIGO_CARGO==7)


#ordering electoral coalitions

state_dep_2006 <- mutate(state_dep_2006, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

state_dep_2006$idleg <-paste0(state_dep_2006$SIGLA_UF,state_dep_2006$CODIGO_CARGO, state_dep_2006$sq_legenda2)

state_dep_2006 <- state_dep_2006 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

state_dep_2006 <-mutate(state_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

state_dep_2006$idleg2 <-paste0(state_dep_2006$idleg, state_dep_2006$resultado2)

state_dep_2006 <- state_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

state_dep_2006 <-mutate(state_dep_2006, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

state_dep_2006 <- state_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

state_dep_2006<-state_dep_2006%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


state_dep_2006<-state_dep_2006%>%
  filter(!(is.na(VOTOS)))



#### DISTRITAL DEPUTY ####

#filter to only distrital representatives

distrital_dep_2006<- cand_2006v2 %>%
  filter(CODIGO_CARGO==8)


#ordering electoral coalitions

distrital_dep_2006 <- mutate(distrital_dep_2006, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

distrital_dep_2006$idleg <-paste0(distrital_dep_2006$SIGLA_UF,distrital_dep_2006$CODIGO_CARGO, distrital_dep_2006$sq_legenda2)

distrital_dep_2006 <- distrital_dep_2006 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

distrital_dep_2006 <-mutate(distrital_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))


distrital_dep_2006$idleg2 <-paste0(distrital_dep_2006$idleg, distrital_dep_2006$resultado2)

distrital_dep_2006 <- distrital_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

distrital_dep_2006 <-mutate(distrital_dep_2006, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

distrital_dep_2006 <- distrital_dep_2006 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

distrital_dep_2006<-distrital_dep_2006%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)



distrital_dep_2006<-distrital_dep_2006%>%
  filter(!(is.na(VOTOS)))

#######Elections 2010 ###########


library(dplyr)

vot_2010 <- vot_1998_2014[[4]]
cand_2010 <- cand_1998_2014v2[[4]]

vot_2010<- vot_2010 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_10 <- vot_2010 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UF,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2010v2 <- cand_2010 %>% left_join(cand_voto_10, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2010, cand_voto_10, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2010v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2010v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))



#### FEDERAL DEPUTY ####

#filter to only Federal representatives

fed_dep_2010<- cand_2010v2 %>%
  filter(CODIGO_CARGO==6)


#ordering electoral coalitions

fed_dep_2010 <- mutate(fed_dep_2010, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

fed_dep_2010$idleg <-paste0(fed_dep_2010$SIGLA_UF,fed_dep_2010$CODIGO_CARGO, fed_dep_2010$sq_legenda2)

fed_dep_2010 <- fed_dep_2010 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

fed_dep_2010 <-mutate(fed_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

fed_dep_2010$idleg2 <-paste0(fed_dep_2010$idleg, fed_dep_2010$resultado2)

fed_dep_2010 <- fed_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

fed_dep_2010 <-mutate(fed_dep_2010, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

fed_dep_2010 <- fed_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

fed_dep_2010<-fed_dep_2010%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)



fed_dep_2010<-fed_dep_2010%>%
  filter(!(is.na(VOTOS)))

#### STATE DEPUTY ####

#filter to only Federal representatives

state_dep_2010<- cand_2010v2 %>%
  filter(CODIGO_CARGO==7)


#ordering electoral coalitions

state_dep_2010 <- mutate(state_dep_2010, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

state_dep_2010$idleg <-paste0(state_dep_2010$SIGLA_UF,state_dep_2010$CODIGO_CARGO, state_dep_2010$sq_legenda2)

state_dep_2010 <- state_dep_2010 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

state_dep_2010 <-mutate(state_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

state_dep_2010$idleg2 <-paste0(state_dep_2010$idleg, state_dep_2010$resultado2)

state_dep_2010 <- state_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

state_dep_2010 <-mutate(state_dep_2010, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

state_dep_2010 <- state_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

state_dep_2010<-state_dep_2010%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

state_dep_2010<-state_dep_2010%>%
  filter(!(is.na(VOTOS)))


#### DISTRITAL DEPUTY ####

#filter to only distrital representatives

distrital_dep_2010<- cand_2010v2 %>%
  filter(CODIGO_CARGO==8)


#ordering electoral coalitions

distrital_dep_2010 <- mutate(distrital_dep_2010, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

distrital_dep_2010$idleg <-paste0(distrital_dep_2010$SIGLA_UF,distrital_dep_2010$CODIGO_CARGO, distrital_dep_2010$sq_legenda2)

distrital_dep_2010 <- distrital_dep_2010 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

distrital_dep_2010 <-mutate(distrital_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))


distrital_dep_2010$idleg2 <-paste0(distrital_dep_2010$idleg, distrital_dep_2010$resultado2)

distrital_dep_2010 <- distrital_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

distrital_dep_2010 <-mutate(distrital_dep_2010, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

distrital_dep_2010 <- distrital_dep_2010 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

distrital_dep_2010<-distrital_dep_2010%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


distrital_dep_2010<-distrital_dep_2010%>%
  filter(!(is.na(VOTOS)))


#######Elections 2014 ###########


library(dplyr)

vot_2014 <- vot_1998_2014[[5]]
cand_2014 <- cand_1998_2014v2[[5]]

vot_2014<- vot_2014 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_14 <- vot_2014 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UF,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2014v2 <- cand_2014 %>% left_join(cand_voto_14, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2014, cand_voto_14, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UF", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2014v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2014v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))


#### FEDERAL DEPUTY ####

#filter to only Federal representatives

fed_dep_2014<- cand_2014v2 %>%
  filter(CODIGO_CARGO==6)


#ordering electoral coalitions

fed_dep_2014 <- mutate(fed_dep_2014, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

fed_dep_2014$idleg <-paste0(fed_dep_2014$SIGLA_UF,fed_dep_2014$CODIGO_CARGO, fed_dep_2014$sq_legenda2)

fed_dep_2014 <- fed_dep_2014 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

fed_dep_2014 <-mutate(fed_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

fed_dep_2014$idleg2 <-paste0(fed_dep_2014$idleg, fed_dep_2014$resultado2)

fed_dep_2014 <- fed_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

fed_dep_2014 <-mutate(fed_dep_2014, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

fed_dep_2014 <- fed_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

fed_dep_2014<-fed_dep_2014%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

fed_dep_2014<-fed_dep_2014%>%
  filter(!(is.na(VOTOS)))


#### STATE DEPUTY ####

#filter to only Federal representatives

state_dep_2014<- cand_2014v2 %>%
  filter(CODIGO_CARGO==7)


#ordering electoral coalitions

state_dep_2014 <- mutate(state_dep_2014, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

state_dep_2014$idleg <-paste0(state_dep_2014$SIGLA_UF,state_dep_2014$CODIGO_CARGO, state_dep_2014$sq_legenda2)

state_dep_2014 <- state_dep_2014 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

state_dep_2014 <-mutate(state_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

state_dep_2014$idleg2 <-paste0(state_dep_2014$idleg, state_dep_2014$resultado2)

state_dep_2014 <- state_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

state_dep_2014 <-mutate(state_dep_2014, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

state_dep_2014 <- state_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

state_dep_2014<-state_dep_2014%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


state_dep_2014<-state_dep_2014%>%
  filter(!(is.na(VOTOS)))

#### DISTRITAL DEPUTY ####

#filter to only distrital representatives

distrital_dep_2014<- cand_2014v2 %>%
  filter(CODIGO_CARGO==8)


#ordering electoral coalitions

distrital_dep_2014 <- mutate(distrital_dep_2014, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

distrital_dep_2014$idleg <-paste0(distrital_dep_2014$SIGLA_UF,distrital_dep_2014$CODIGO_CARGO, distrital_dep_2014$sq_legenda2)

distrital_dep_2014 <- distrital_dep_2014 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

distrital_dep_2014 <-mutate(distrital_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))


distrital_dep_2014$idleg2 <-paste0(distrital_dep_2014$idleg, distrital_dep_2014$resultado2)

distrital_dep_2014 <- distrital_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

distrital_dep_2014 <-mutate(distrital_dep_2014, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

distrital_dep_2014 <- distrital_dep_2014 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))


distrital_dep_2014<-distrital_dep_2014%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


distrital_dep_2014<-distrital_dep_2014%>%
  filter(!(is.na(VOTOS)))
 

################# CITY COUNSIL ELECTIONS ###################


load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/vot_2000_2016.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_2000_2016v2.RData")

#######Elections 2004 ###########


library(dplyr)

vot_2004 <- vot_2000_2016[[2]]
cand_2004 <- cand_2000_2016v2[[2]]

vot_2004<- vot_2004 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_04 <- vot_2004 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UE,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2004v2 <- cand_2004 %>% left_join(cand_voto_04, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2004, cand_voto_04, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2004v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2004v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))


#### VEREADOR ####

#filter to only Federal representatives

ver_2004<- cand_2004v2 %>%
  filter(CODIGO_CARGO==13)


#ordering electoral coalitions

ver_2004 <- mutate(ver_2004, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

ver_2004$idleg <-paste0(ver_2004$SIGLA_UE,ver_2004$CODIGO_CARGO, ver_2004$sq_legenda2)

ver_2004 <- ver_2004 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

ver_2004 <-mutate(ver_2004, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

ver_2004$idleg2 <-paste0(ver_2004$idleg, ver_2004$resultado2)

ver_2004 <- ver_2004 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

ver_2004 <-mutate(ver_2004, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

ver_2004 <- ver_2004 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))


ver_2004<-ver_2004%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

ver_2004<-ver_2004%>%
  filter(!(is.na(VOTOS)))



#######Elections 2008 ###########


library(dplyr)

vot_2008 <- vot_2000_2016[[3]]
cand_2008 <- cand_2000_2016v2[[3]]

vot_2008<- vot_2008 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_08 <- vot_2008 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UE,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2008v2 <- cand_2008 %>% left_join(cand_voto_08, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2008, cand_voto_08, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2008v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2008v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))

######
###### necessidade de filtrar pelos candidatos repeditos
######

#### VEREADOR ####

#filter to only Federal representatives

ver_2008<- cand_2008v2 %>%
  filter(CODIGO_CARGO==13)


#ordering electoral coalitions

ver_2008 <- mutate(ver_2008, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

ver_2008$idleg <-paste0(ver_2008$SIGLA_UE,ver_2008$CODIGO_CARGO, ver_2008$sq_legenda2)

ver_2008 <- ver_2008 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

ver_2008 <-mutate(ver_2008, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

ver_2008$idleg2 <-paste0(ver_2008$idleg, ver_2008$resultado2)

ver_2008 <- ver_2008 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

ver_2008 <-mutate(ver_2008, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

ver_2008 <- ver_2008 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

ver_2008<-ver_2008%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)

 
ver_2008<-ver_2008%>%
  filter(!(is.na(VOTOS)))


#######Elections 2012 ###########


library(dplyr)

vot_2012 <- vot_2000_2016[[4]]
cand_2012 <- cand_2000_2016v2[[4]]

vot_2012<- vot_2012 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_12 <- vot_2012 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UE,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2012v2 <- cand_2012 %>% left_join(cand_voto_12, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2012, cand_voto_12, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2012v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2012v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))


#### VEREADOR ####

#filter to only Federal representatives

ver_2012<- cand_2012v2 %>%
  filter(CODIGO_CARGO==13)


#ordering electoral coalitions

ver_2012 <- mutate(ver_2012, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

ver_2012$idleg <-paste0(ver_2012$SIGLA_UE,ver_2012$CODIGO_CARGO, ver_2012$sq_legenda2)

ver_2012 <- ver_2012 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

ver_2012 <-mutate(ver_2012, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

ver_2012$idleg2 <-paste0(ver_2012$idleg, ver_2012$resultado2)

ver_2012 <- ver_2012 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

ver_2012 <-mutate(ver_2012, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

ver_2012 <- ver_2012 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))

ver_2012<-ver_2012%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


ver_2012<-ver_2012%>%
  filter(!(is.na(VOTOS)))



#######Elections 2016 ###########


library(dplyr)

vot_2016 <- vot_2000_2016[[5]]
cand_2016 <- cand_2000_2016v2[[5]]

vot_2016<- vot_2016 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_16 <- vot_2016 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UE,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
cand_2016v2 <- cand_2016 %>% left_join(cand_voto_16, by=c("SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))

#Debugging #which do not merge?
bugs <- anti_join(cand_2016, cand_voto_16, by=c( "SEQUENCIAL_CANDIDATO", "SIGLA_UE", "CODIGO_CARGO", "NUM_TURNO"))
table(bugs$DESC_SIT_TOT_TURNO)
table(bugs$DESCRICAO_CARGO)
table(bugs$DESC_SIT_TOT_TURNO, bugs$DESCRICAO_CARGO)
table(bugs$DES_SITUACAO_CANDIDATURA, bugs$DESCRICAO_CARGO)

#Verifying duplicity in candidates

problems <- cand_2016v2 %>% group_by(NUM_TURNO, NUMERO_CANDIDATO, CODIGO_CARGO, SIGLA_UE) %>%
  summarise(total = n()) %>% filter(total > 1)

casos <- cand_2016v2 %>% right_join(problems, by = c("NUM_TURNO", 
                                                     "NUMERO_CANDIDATO", "CODIGO_CARGO", "SIGLA_UE"))



#### VEREADOR ####

#filter to only Federal representatives

ver_2016<- cand_2016v2 %>%
  filter(CODIGO_CARGO==13)


#ordering electoral coalitions

ver_2016 <- mutate(ver_2016, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

ver_2016$idleg <-paste0(ver_2016$SIGLA_UE,ver_2016$CODIGO_CARGO, ver_2016$sq_legenda2)

ver_2016 <- ver_2016 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

ver_2016 <-mutate(ver_2016, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR M?DIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","N?o eleito"))))

ver_2016$idleg2 <-paste0(ver_2016$idleg, ver_2016$resultado2)

ver_2016 <- ver_2016 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

ver_2016 <-mutate(ver_2016, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

ver_2016 <- ver_2016 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))


ver_2016<-ver_2016%>%
  dplyr::select(DATA_GERACAO, HORA_GERACAO, ANO_ELEICAO.x, NUM_TURNO, DESCRICAO_CARGO.x, SIGLA_UF, SIGLA_UE, DESCRICAO_UE, 
                CODIGO_CARGO, DESCRICAO_CARGO.x, NOME_CANDIDATO.x, SEQUENCIAL_CANDIDATO, NUMERO_CANDIDATO, CPF_CANDIDATO, 
                NOME_URNA_CANDIDATO.x, COD_SITUACAO_CANDIDATURA, DES_SITUACAO_CANDIDATURA, NUMERO_PARTIDO, SIGLA_PARTIDO.x,
                NOME_PARTIDO,CODIGO_LEGENDA, SIGLA_LEGENDA, COMPOSICAO_LEGENDA, NOME_COLIGACAO, CODIGO_OCUPACAO, DESCRICAO_OCUPACAO,
                DATA_NASCIMENTO, NUM_TITULO_ELEITORAL_CANDIDATO, IDADE_DATA_ELEICAO, CODIGO_SEXO, COD_GRAU_INSTRUCAO, CODIGO_ESTADO_CIVIL,
                DESCRICAO_ESTADO_CIVIL, CODIGO_NACIONALIDADE, DESCRICAO_NACIONALIDADE, SIGLA_UF_NASCIMENTO, CODIGO_MUNICIPIO_NASCIMENTO,
                NOME_MUNICIPIO_NASCIMENTO, DESPESA_MAX_CAMPANHA, COD_SIT_TOT_TURNO, DESC_SIT_TOT_TURNO,VOTOS,sq_legenda2,idleg, idleg2, rank, rank2, resultado2, prim_supl, flag)


ver_2016<-ver_2016%>%
  filter(!(is.na(VOTOS)))


######################################## 

#saving data from all elections


distrital_dep_2002_2014 <- list(distrital_dep_2002, distrital_dep_2006, distrital_dep_2010, distrital_dep_2014)
save(distrital_dep_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/distrital_dep_2002_2014.RData")

state_dep_2002_2014 <- list(state_dep_2002, state_dep_2006, state_dep_2010, state_dep_2014)
save(state_dep_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/state_dep_2002_2014.RData")

fed_dep_2002_2014 <- list(fed_dep_2002, fed_dep_2006, fed_dep_2010, fed_dep_2014)
save(fed_dep_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/fed_dep_2002_2014.RData")

ver_2004_2016 <- list(ver_2004, ver_2008, ver_2012, ver_2016)
save(ver_2004_2016, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/ver_2004_2016.RData")

################################################################################
################################################################################
######  4. Downloading, organizing and cleaning data about partisan votes ######
#1. Downloading 
#2. Consolidating
#3. Organizing
###################################################################
###################################################################

#1. Downloading data from partisan voting
url_votpar98 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_1998.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_1998.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_1998")
votpar_1998 <- get_tse(url_votpar98, file_d, file_un)

url_votpar00 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2000.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2000.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2000")
votpar_2000 <- get_tse(url_votpar00, file_d, file_un)

url_votpar02 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2002.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2002.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2002")
votpar_2002 <- get_tse(url_votpar02, file_d, file_un)

url_votpar04 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2004.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2004.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2004")
votpar_2004 <- get_tse(url_votpar04, file_d, file_un)

url_votpar06 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2006.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2006.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2006")
votpar_2006 <- get_tse(url_votpar06, file_d, file_un)

url_votpar08 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2008.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2008.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2008")
votpar_2008 <- get_tse(url_votpar08, file_d, file_un)

url_votpar10 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2010.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2010.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2010")
votpar_2010 <- get_tse(url_votpar10, file_d, file_un)

url_votpar12 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2012.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2012.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2012")
votpar_2012 <- get_tse(url_votpar12, file_d, file_un)

url_votpar14 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2014.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2014.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2014")
votpar_2014 <- get_tse(url_votpar14, file_d, file_un)

url_votpar16 <- "http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2016.zip"
file_d <- paste0(dir_d, "original_data/votacao_partido/votacao_partido_munzona_2016.zip")
file_un <- paste0(dir_d, "original_unzipped/votacao_partido/votacao_partido_munzona_2016")
votpar_2016 <- get_tse(url_votpar16, file_d, file_un)

########################
#2. Consolidating data from partisan voting

ufs_n <- c("AC", "AL", "AP", "AM", "BA", "BR",   
           "CE", "DF", "ES", "GO", "MA", "MT", "MS",
           "MG", "PA", "PB", "PR", "PE", "PI", "RJ",
           "RN", "RS", "RO","RR","SC", "SP", "SE", "TO", "ZZ")

ufs <- c("AC", "AL", "AP", "AM", "BA", "BR",   
         "CE", "DF", "ES", "GO", "MA", "MT", "MS",
         "MG", "PA", "PB", "PR", "PE", "PI", "RJ",
         "RN", "RS", "RO","RR","SC", "SP", "SE", "TO", "ZZ")

#partidos labels

labels_pre2014 <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                    "SIGLA_UF", "SIGLA_UE", "CODIGO_MUNICIPIO", "NOME_MUNICIPIO", "NUMERO_ZONA",
                    "CODIGO_CARGO", "DESCRICAO_CARGO", "TIPO_LEGENDA", "NOME_COLIGACAO", "COMPOSICAO_LEGENDA",
                    "SIGLA_PARTIDO", "NUMERO_PARTIDO", "NOME_PARTIDO", "QTDE_VOTOS_NOMINAIS","QTDE_VOTOS_LEGENDA",
                    "SEQUENCIAL_COLIGACAO")

labels_2014 <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", "NUM_TURNO", "DESCRICAO_ELEICAO",
                 "SIGLA_UF", "SIGLA_UE", "CODIGO_MUNICIPIO", "NOME_MUNICIPIO", "NUMERO_ZONA",
                 "CODIGO_CARGO", "DESCRICAO_CARGO", "TIPO_LEGENDA", "NOME_COLIGACAO", "COMPOSICAO_LEGENDA",
                 "SIGLA_PARTIDO", "NUMERO_PARTIDO", "NOME_PARTIDO", "QTDE_VOTOS_NOMINAIS","QTDE_VOTOS_LEGENDA","TRANSITO",
                 "SEQUENCIAL_COLIGACAO")

#Voting 1998
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_1998/votacao_partido_munzona_1998_",
                        ufs_n[!ufs_n %in% c("BR")], ".txt"))
votpar_1998 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_1998 <- do.call("rbind", votpar_1998)
names(votpar_1998) <- labels_pre2014
votpar_1998 <- as_tibble(votpar_1998)

#Voting 2002
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2002/votacao_partido_munzona_2002_",
                        ufs_n[!ufs_n %in% c("BR")], ".txt"))
votpar_2002 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2002 <- do.call("rbind", votpar_2002)
names(votpar_2002) <- labels_pre2014
votpar_2002 <- as_tibble(votpar_2002)

#Voting data 2006
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2006/votacao_partido_munzona_2006_",
                        ufs, ".txt"))
votpar_2006 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2006 <- do.call("rbind", votpar_2006)
names(votpar_2006) <- labels_pre2014
votpar_2006 <- as_tibble(votpar_2006)

#Voting data 2010
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2010/votacao_partido_munzona_2010_",
                        ufs, ".txt"))
votpar_2010 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors=F, fill = T, fileEncoding = "windows-1252") 
votpar_2010 <- do.call("rbind", votpar_2010)
names(votpar_2010) <- labels_pre2014
votpar_2010 <- as_tibble(votpar_2010)

#voting data 2014
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2014/votacao_partido_munzona_2014_",
                        ufs_n[!ufs_n %in% c("ZZ")], ".txt"))
votpar_2014 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2014 <- do.call("rbind", votpar_2014)
names(votpar_2014) <- labels_2014
votpar_2014 <- as_tibble(votpar_2014)

votpar_1998_2014 <- list(votpar_1998, votpar_2002, votpar_2006, votpar_2010, votpar_2014)
save(votpar_1998_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votpar_1998_2014.RData")


################ local elections

#Voting 2000
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2000/votacao_partido_munzona_2000_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
votpar_2000 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2000 <- do.call("rbind", votpar_2000)
names(votpar_2000) <- labels_pre2014
votpar_2000 <- as_tibble(votpar_2000)

#Voting data 2004
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2004/votacao_partido_munzona_2004_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
votpar_2004 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2004 <- do.call("rbind", votpar_2004)
names(votpar_2004) <- labels_pre2014
votpar_2004 <- as_tibble(votpar_2004)

#Voting data 2008
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2008/votacao_partido_munzona_2008_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
votpar_2008 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors=F, fill = T, fileEncoding = "windows-1252") 
votpar_2008 <- do.call("rbind", votpar_2008)
names(votpar_2008) <- labels_pre2014
votpar_2008 <- as_tibble(votpar_2008)

#voting data 2012
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2012/votacao_partido_munzona_2012_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
votpar_2012 <- lapply(files, read.table, sep = ";", 
                      header = F, stringsAsFactors = F, fill = T, fileEncoding = "windows-1252") 
votpar_2012 <- do.call("rbind", votpar_2012)
names(votpar_2012) <- labels_pre2014
votpar_2012 <- as_tibble(votpar_2012)


#voting data 2016
files <- as.list(paste0("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votacao_partido/votacao_partido_munzona_2016/votacao_partido_munzona_2016_",
                        ufs_n[!ufs_n %in% c("BR", "ZZ", "DF")], ".txt"))
votpar_2016 <- lapply(files, read.table, sep=";", 
                      header=F, stringsAsFactors=F, fill = T, fileEncoding = "latin1") 
votpar_2016 <- do.call("rbind", votpar_2016)
names(votpar_2016) <- labels_2014
votpar_2016 <- as_tibble(votpar_2016)

votpar_2000_2016 <- list(votpar_2000, votpar_2004, votpar_2008, votpar_2012, votpar_2016)
save(votpar_2000_2016, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/votpar_2000_2016.RData")

#################

#3. Organizing partisan Voting

# consolidanting  votes per party

votpar_ue_2002 <- votpar_2002  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA))

votpar_ue_2004 <- votpar_2004  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2006 <- votpar_2006  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2008 <- votpar_2008  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2010 <- votpar_2010  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2012 <- votpar_2012  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2014 <- votpar_2014  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

votpar_ue_2016 <- votpar_2016  %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, CODIGO_CARGO) %>%
  summarise(VOTOS= sum(QTDE_VOTOS_LEGENDA)) 

### auxilia data set from parties in order to get the right idleg

#federal dep
aux_fed_dep_part_2002 <- fed_dep_2002 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_fed_dep_part_2006 <- fed_dep_2006 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_fed_dep_part_2010 <- fed_dep_2010 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_fed_dep_part_2014 <- fed_dep_2014 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())

#state dep
aux_state_dep_part_2002 <- state_dep_2002 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_state_dep_part_2006 <- state_dep_2006 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_state_dep_part_2010 <- state_dep_2010 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_state_dep_part_2014 <- state_dep_2014 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())

#distrital
aux_distrital_dep_part_2002 <- distrital_dep_2002 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_distrital_dep_part_2006 <- distrital_dep_2006 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_distrital_dep_part_2010 <- distrital_dep_2010 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())
aux_distrital_dep_part_2014 <- distrital_dep_2014 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UF, idleg)%>%
  summarise(total=n())

#vereador
aux_ver_part_2004 <- ver_2004 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, idleg)%>%
  summarise(total=n())
aux_ver_part_2008 <- ver_2008 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, idleg)%>%
  summarise(total=n())
aux_ver_part_2012 <- ver_2012 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, idleg)%>%
  summarise(total=n())
aux_ver_part_2016 <- ver_2016 %>%
  group_by(NUMERO_PARTIDO, SIGLA_UE, idleg)%>%
  summarise(total=n())

#########################################
# merging information of partisan votes and idleg by type of office

#federal deputy
fed_dep_votpar_ue_2002 <- merge(votpar_ue_2002, aux_fed_dep_part_2002, by=c("NUMERO_PARTIDO","SIGLA_UF"))
fed_dep_votpar_ue_2002 <- fed_dep_votpar_ue_2002 %>%
  filter(CODIGO_CARGO==6)

fed_dep_votpar_ue_2006 <- merge(votpar_ue_2006, aux_fed_dep_part_2006, by=c("NUMERO_PARTIDO","SIGLA_UF"))
fed_dep_votpar_ue_2006 <- fed_dep_votpar_ue_2006 %>%
  filter(CODIGO_CARGO==6)

fed_dep_votpar_ue_2010 <- merge(votpar_ue_2010, aux_fed_dep_part_2010, by=c("NUMERO_PARTIDO","SIGLA_UF"))
fed_dep_votpar_ue_2010 <- fed_dep_votpar_ue_2010 %>%
  filter(CODIGO_CARGO==6)

fed_dep_votpar_ue_2014 <- merge(votpar_ue_2014, aux_fed_dep_part_2014, by=c("NUMERO_PARTIDO","SIGLA_UF"))
fed_dep_votpar_ue_2014 <- fed_dep_votpar_ue_2014 %>%
  filter(CODIGO_CARGO==6)

# merging information of partisan votes and idleg by type of office

#state deputy

state_dep_votpar_ue_2002 <- merge(votpar_ue_2002, aux_state_dep_part_2002, by=c("NUMERO_PARTIDO","SIGLA_UF"))
state_dep_votpar_ue_2002 <- state_dep_votpar_ue_2002 %>%
  filter(CODIGO_CARGO==7)

state_dep_votpar_ue_2006 <- merge(votpar_ue_2006, aux_state_dep_part_2006, by=c("NUMERO_PARTIDO","SIGLA_UF"))
state_dep_votpar_ue_2006 <- state_dep_votpar_ue_2006 %>%
  filter(CODIGO_CARGO==7)

state_dep_votpar_ue_2010 <- merge(votpar_ue_2010, aux_state_dep_part_2010, by=c("NUMERO_PARTIDO","SIGLA_UF"))
state_dep_votpar_ue_2010 <- state_dep_votpar_ue_2010 %>%
  filter(CODIGO_CARGO==7)

state_dep_votpar_ue_2014 <- merge(votpar_ue_2014, aux_state_dep_part_2014, by=c("NUMERO_PARTIDO","SIGLA_UF"))
state_dep_votpar_ue_2014 <- state_dep_votpar_ue_2014 %>%
  filter(CODIGO_CARGO==7)


# merging information of partisan votes and idleg by type of office

#distrital deputy

distrital_dep_votpar_ue_2002 <- merge(votpar_ue_2002, aux_distrital_dep_part_2002, by=c("NUMERO_PARTIDO","SIGLA_UF"))
distrital_dep_votpar_ue_2002 <- distrital_dep_votpar_ue_2002 %>%
  filter(CODIGO_CARGO==8)

distrital_dep_votpar_ue_2006 <- merge(votpar_ue_2006, aux_distrital_dep_part_2006, by=c("NUMERO_PARTIDO","SIGLA_UF"))
distrital_dep_votpar_ue_2006 <- distrital_dep_votpar_ue_2006 %>%
  filter(CODIGO_CARGO==8)

distrital_dep_votpar_ue_2010 <- merge(votpar_ue_2010, aux_distrital_dep_part_2010, by=c("NUMERO_PARTIDO","SIGLA_UF"))
distrital_dep_votpar_ue_2010 <- distrital_dep_votpar_ue_2010 %>%
  filter(CODIGO_CARGO==8)

distrital_dep_votpar_ue_2014 <- merge(votpar_ue_2014, aux_distrital_dep_part_2014, by=c("NUMERO_PARTIDO","SIGLA_UF"))
distrital_dep_votpar_ue_2014 <- distrital_dep_votpar_ue_2014 %>%
  filter(CODIGO_CARGO==8)

# merging information of partisan votes and idleg by type of office

# vereador

ver_votpar_ue_2004 <- merge(votpar_ue_2004, aux_ver_part_2004, by=c("NUMERO_PARTIDO","SIGLA_UE"))
ver_votpar_ue_2004 <- ver_votpar_ue_2004 %>%
  filter(CODIGO_CARGO==13)

ver_votpar_ue_2008 <- merge(votpar_ue_2008, aux_ver_part_2008, by=c("NUMERO_PARTIDO","SIGLA_UE"))
ver_votpar_ue_2008 <- ver_votpar_ue_2008 %>%
  filter(CODIGO_CARGO==13)

ver_votpar_ue_2012 <- merge(votpar_ue_2012, aux_ver_part_2012, by=c("NUMERO_PARTIDO","SIGLA_UE"))
ver_votpar_ue_2012 <- ver_votpar_ue_2012 %>%
  filter(CODIGO_CARGO==13)

ver_votpar_ue_2016 <- merge(votpar_ue_2016, aux_ver_part_2016, by=c("NUMERO_PARTIDO","SIGLA_UE"))
ver_votpar_ue_2016 <- ver_votpar_ue_2016 %>%
  filter(CODIGO_CARGO==13)

# binding rows in order to make a dataset with partisan votes
# federal deputy
fed_dep_part_2002 <-bind_rows(fed_dep_2002, fed_dep_votpar_ue_2002)
fed_dep_part_2002$total<-NULL

fed_dep_part_2006 <-bind_rows(fed_dep_2006, fed_dep_votpar_ue_2006)
fed_dep_part_2006$total<-NULL

fed_dep_part_2010 <-bind_rows(fed_dep_2010, fed_dep_votpar_ue_2010)
fed_dep_part_2010$total<-NULL

fed_dep_part_2014 <-bind_rows(fed_dep_2014, fed_dep_votpar_ue_2014)
fed_dep_part_2014$total<-NULL


# binding rows in order to make a dataset with partisan votes
# state deputy
state_dep_part_2002 <-bind_rows(state_dep_2002, state_dep_votpar_ue_2002)
state_dep_part_2002$total<-NULL

state_dep_part_2006 <-bind_rows(state_dep_2006, state_dep_votpar_ue_2006)
state_dep_part_2006$total<-NULL

state_dep_part_2010 <-bind_rows(state_dep_2010, state_dep_votpar_ue_2010)
state_dep_part_2010$total<-NULL

state_dep_part_2014 <-bind_rows(state_dep_2014, state_dep_votpar_ue_2014)
state_dep_part_2014$total<-NULL


# binding rows in order to make a dataset with partisan votes
# distrital deputy
distrital_dep_part_2002 <-bind_rows(distrital_dep_2002, distrital_dep_votpar_ue_2002)
distrital_dep_part_2002$total<-NULL

distrital_dep_part_2006 <-bind_rows(distrital_dep_2006, distrital_dep_votpar_ue_2006)
distrital_dep_part_2006$total<-NULL

distrital_dep_part_2010 <-bind_rows(distrital_dep_2010, distrital_dep_votpar_ue_2010)
distrital_dep_part_2010$total<-NULL

distrital_dep_part_2014 <-bind_rows(distrital_dep_2014, distrital_dep_votpar_ue_2014)
distrital_dep_part_2014$total<-NULL


# binding rows in order to make a dataset with partisan votes
# vereador
ver_part_2004 <-bind_rows(ver_2004, ver_votpar_ue_2004)
ver_part_2004$total<-NULL

ver_part_2008 <-bind_rows(ver_2008, ver_votpar_ue_2008)
ver_part_2008$total<-NULL

ver_part_2012 <-bind_rows(ver_2012, ver_votpar_ue_2012)
ver_part_2012$total<-NULL

ver_part_2016 <-bind_rows(ver_2016, ver_votpar_ue_2016)
ver_part_2016$total<-NULL

######################################## 

# Saving datasets with partisan votes

distrital_dep_part_2002_2014 <- list(distrital_dep_part_2002, distrital_dep_part_2006, distrital_dep_part_2010, distrital_dep_part_2014)
save(distrital_dep_part_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/distrital_dep_part_2002_2014.RData")

state_dep_part_2002_2014 <- list(state_dep_part_2002, state_dep_part_2006, state_dep_part_2010, state_dep_part_2014)
save(state_dep_part_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/state_dep_part_2002_2014.RData")

fed_dep_part_2002_2014 <- list(fed_dep_part_2002, fed_dep_part_2006, fed_dep_part_2010, fed_dep_part_2014)
save(fed_dep_part_2002_2014, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/fed_dep_part_2002_2014.RData")

ver_part_2004_2016 <- list(ver_part_2004, ver_part_2008, ver_part_2012, ver_part_2016)
save(ver_part_2004_2016, file = "//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/ver_part_2004_2016.RData")



###################################################################
###################################################################
###########  5. Preliminary tests                       ###########
#1. Loading data
#2. Number of elected candidates test
#3. Negative votes test
###################################################################
###################################################################


#1. Loading Data
rm(list=ls())

load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/distrital_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/state_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/fed_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/ver_2004_2016.RData")


load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/distrital_dep_part_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/state_dep_part_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/fed_dep_part_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/ver_part_2004_2016.RData")



#######################################
#fed dep

fed_dep_2002 <- fed_dep_2002_2014[[1]]
fed_dep_2006 <- fed_dep_2002_2014[[2]]
fed_dep_2010 <- fed_dep_2002_2014[[3]]
fed_dep_2014 <- fed_dep_2002_2014[[4]]


#state dep

state_dep_2002 <- state_dep_2002_2014[[1]]
state_dep_2006 <- state_dep_2002_2014[[2]]
state_dep_2010 <- state_dep_2002_2014[[3]]
state_dep_2014 <- state_dep_2002_2014[[4]]

#distrital dep

distrital_dep_2002 <- distrital_dep_2002_2014[[1]]
distrital_dep_2006 <- distrital_dep_2002_2014[[2]]
distrital_dep_2010 <- distrital_dep_2002_2014[[3]]
distrital_dep_2014 <- distrital_dep_2002_2014[[4]]

fed_dep_part_2002 <- fed_dep_part_2002_2014[[1]]
fed_dep_part_2006 <- fed_dep_part_2002_2014[[2]]
fed_dep_part_2010 <- fed_dep_part_2002_2014[[3]]
fed_dep_part_2014 <- fed_dep_part_2002_2014[[4]]


#state dep_part

state_dep_part_2002 <- state_dep_part_2002_2014[[1]]
state_dep_part_2006 <- state_dep_part_2002_2014[[2]]
state_dep_part_2010 <- state_dep_part_2002_2014[[3]]
state_dep_part_2014 <- state_dep_part_2002_2014[[4]]

#distrital dep_part

distrital_dep_part_2002 <- distrital_dep_part_2002_2014[[1]]
distrital_dep_part_2006 <- distrital_dep_part_2002_2014[[2]]
distrital_dep_part_2010 <- distrital_dep_part_2002_2014[[3]]
distrital_dep_part_2014 <- distrital_dep_part_2002_2014[[4]]

# vereadores

ver_2004 <- ver_2004_2016[[1]]
ver_2008 <- ver_2004_2016[[2]]
ver_2012 <- ver_2004_2016[[3]]
ver_2016 <- ver_2004_2016[[4]]

#2. Number of elected candidates test
##### Smell checks

# elected

electe_fed_dep_02 <- fed_dep_2002 %>%
  filter(resultado2=="Eleito")
electe_fed_dep_06 <- fed_dep_2006 %>%
  filter(resultado2=="Eleito")
electe_fed_dep_10 <- fed_dep_2010 %>%
  filter(resultado2=="Eleito")
electe_fed_dep_14 <- fed_dep_2014 %>%
  filter(resultado2=="Eleito")

electe_state_dep_02 <- state_dep_2002 %>%
  filter(resultado2=="Eleito")
electe_state_dep_06 <- state_dep_2006 %>%
  filter(resultado2=="Eleito")
electe_state_dep_10 <- state_dep_2010 %>%
  filter(resultado2=="Eleito")
electe_state_dep_14 <- state_dep_2014 %>%
  filter(resultado2=="Eleito")

electe_distrital_dep_02 <- distrital_dep_2002 %>%
  filter(resultado2=="Eleito")
electe_distrital_dep_06 <- distrital_dep_2006 %>%
  filter(resultado2=="Eleito")
electe_distrital_dep_10 <- distrital_dep_2010 %>%
  filter(resultado2=="Eleito")
electe_distrital_dep_14 <- distrital_dep_2014 %>%
  filter(resultado2=="Eleito")

electe_ver_04 <- ver_2004 %>%
  filter(resultado2=="Eleito")
electe_ver_08 <- ver_2008 %>%
  filter(resultado2=="Eleito")
electe_ver_12 <- ver_2012 %>%
  filter(resultado2=="Eleito")
electe_ver_16 <- ver_2016 %>%
  filter(resultado2=="Eleito")

#3. Negative votes test
#### negative votes

vot_fed_dep_02 <- fed_dep_2002 %>%
  filter(VOTOS<0)
vot_fed_dep_06 <- fed_dep_2006 %>%
  filter(VOTOS<0)
vot_fed_dep_10 <- fed_dep_2010 %>%
  filter(VOTOS<0)
vot_fed_dep_14 <- fed_dep_2014 %>%
  filter(VOTOS<0)

vot_state_dep_02 <- state_dep_2002 %>%
  filter(VOTOS<0)
vot_state_dep_06 <- state_dep_2006 %>%
  filter(VOTOS<0)
vot_state_dep_10 <- state_dep_2010 %>%
  filter(VOTOS<0)
vot_state_dep_14 <- state_dep_2014 %>%
  filter(VOTOS<0)

vot_distrital_dep_02 <- distrital_dep_2002 %>%
  filter(VOTOS<0)
vot_distrital_dep_06 <- distrital_dep_2006 %>%
  filter(VOTOS<0)
vot_distrital_dep_10 <- distrital_dep_2010 %>%
  filter(VOTOS<0)
vot_distrital_dep_14 <- distrital_dep_2014 %>%
  filter(VOTOS<0)

vot_ver_04 <- ver_2004 %>%
  filter(VOTOS<0)
vot_ver_08 <- ver_2008 %>%
  filter(VOTOS<0)
vot_ver_12 <- ver_2012 %>%
  filter(VOTOS<0)
vot_ver_16 <- ver_2016 %>%
  filter(VOTOS<0)

###################################################################
###################################################################
########  6. Functions to generate RDD Datasets            ########
#0. Loading Data
#1. Function Percentage of Last Elected Candidates 
#2. Function Distance of average of votes from last elected and first suplente
#3. Function Sum (IN DEVELOPMENT)
#4. Function Boas and Hidalgo - airwaves 2011
#5. Function State Share
#6. Function Coalition Share
#7. Function Rank 
###################################################################
###################################################################


#0. Loading Data
rm(list=ls())

load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/distrital_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/state_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/fed_dep_2002_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch//Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/ver_2004_2016.RData")

#######################################

#fed dep

fed_dep_2002 <- fed_dep_2002_2014[[1]]
fed_dep_2006 <- fed_dep_2002_2014[[2]]
fed_dep_2010 <- fed_dep_2002_2014[[3]]
fed_dep_2014 <- fed_dep_2002_2014[[4]]


#state dep

state_dep_2002 <- state_dep_2002_2014[[1]]
state_dep_2006 <- state_dep_2002_2014[[2]]
state_dep_2010 <- state_dep_2002_2014[[3]]
state_dep_2014 <- state_dep_2002_2014[[4]]

#distrital dep

distrital_dep_2002 <- distrital_dep_2002_2014[[1]]
distrital_dep_2006 <- distrital_dep_2002_2014[[2]]
distrital_dep_2010 <- distrital_dep_2002_2014[[3]]
distrital_dep_2014 <- distrital_dep_2002_2014[[4]]

# vereadores

ver_2004 <- ver_2004_2016[[1]]
ver_2008 <- ver_2004_2016[[2]]
ver_2012 <- ver_2004_2016[[3]]
ver_2016 <- ver_2004_2016[[4]]


#1. Function Percentage of Last Elected Candidates 
# This function calculates which candidates have at least Y% of votes from the last elected candidate

threshold_simples <- function(data,y){
  
  #last elected
  
  data_ult_ele <- data %>%
    filter(flag==1)
  
  #first suplente
  data_pri_supl <-data%>%
    filter(prim_supl==1)
  
  #selecting only important columns
  
  votos_ult_ele <- data_ult_ele%>%
    dplyr::select(idleg, VOTOS)
  
  votos_pri_supl <- data_pri_supl%>%
    dplyr::select(idleg, VOTOS)
  
  ## merging
  
  votos_thresh <- merge(x=votos_ult_ele, y=votos_pri_supl, by="idleg")
 
  ## calculating threshold
  
  votos_thresh$threshold<- votos_thresh$VOTOS.x * (1-y)
  
  ### eliminating unnecessary columns
  
  votos_thresh<- votos_thresh%>%
    dplyr::select(idleg, threshold)
  
  ### bringing back the threshold
  
  data_final <- merge(x=data, y=votos_thresh, by="idleg")
  
  ### cutting the observations that are not in the threshold
  
  data_final_supl<-data_final%>%
    filter( !(resultado2=="Eleito") & VOTOS>threshold)
  
  data_final_ult<-data_final%>%
    filter(flag==1)
  
  data_final2<-rbind(data_final_ult,data_final_supl)
  
  data_final2<-data_final2%>%
    arrange(desc(idleg, VOTOS))
  
  ### final result
  return(data_final2) 
}


### simple tests
#x <- threshold_simples(data=distrital_dep_2014, y=1)


############
#2. Function Distance of average of votes from last elected and first suplente
# This function calculates which candidates are in the bandwidth 2y 
# The threshold is calculated as the average of the votes from last elected and first suplente
# The distance from the threshold is the running variable

threshold_media <- function(data,y){
  
  #last elected
  
  data_ult_ele <- data %>%
    filter(flag==1)
  
  #first suplente
  data_pri_supl <-data%>%
    filter(prim_supl==1)
  
  #keeping only relevant columns
  
  votos_ult_ele <- data_ult_ele%>%
    dplyr:: select(idleg, VOTOS)
  
  votos_pri_supl <- data_pri_supl%>%
    dplyr:: select(idleg, VOTOS)
  
  ## merging
  
  votos_thresh <- merge(x=votos_ult_ele, y=votos_pri_supl, by="idleg")
  
  ## calculating threshold
  
  votos_thresh$threshold<- ((votos_thresh$VOTOS.x+votos_thresh$VOTOS.y)/2)
  
  ### eliminating unnecessary columns
  
  votos_thresh<- votos_thresh%>%
    dplyr:: select(idleg, threshold)
  
  ### bringing back the threshold
  
  data_final <- merge(x=data, y=votos_thresh, by="idleg")
  
  data_final$distthresh <-(data_final$VOTOS/data_final$threshold)
  
  ### cutting the observations that are not in the threshold
  
  data_final_nele<-data_final%>%
    filter( !(resultado2=="Eleito") & distthresh>(1-y))
  
  data_final_ele<-data_final%>%
    filter( (resultado2=="Eleito") & distthresh<(1+y))
  
  data_final2<-rbind(data_final_ele,data_final_nele)
  
  data_final2<-data_final2%>%
    arrange(desc(idleg, VOTOS))
  
  # final results
  return(data_final2) 
}

#teste
x <- threshold_media(data=distrital_dep_2014, y=0.15)
x2 <- threshold_simples(data=distrital_dep_2014, y=0.15)


################
#3. Function Sum (IN DEVELOPMENT)
# This function calculates which candidates are in the bandwidth 2y 
# The threshold is calculated as the sum of the votes from last elected and first suplente
# This funcion is not completed, in development

threshold_soma <- function(data,y){
  
  #ultimo eleito
  
  data_ult_ele <- data %>%
    dplyr:: filter(flag==1)
  
  #primeiro suplente
  data_pri_supl <-data%>%
    dplyr::filter(prim_supl==1)
  
  ## s? colunas que importam
  
  votos_ult_ele <- data_ult_ele%>%
    dplyr:: select(idleg, VOTOS)
  
  votos_pri_supl <- data_pri_supl%>%
    dplyr:: select(idleg, VOTOS)
  
  ## merging
  
  votos_thresh <- merge(x=votos_ult_ele, y=votos_pri_supl, by="idleg")
  
  ## calculating threshold
  
  votos_thresh$threshold<- ((votos_thresh$VOTOS.x+votos_thresh$VOTOS.y))
  
  ### eliminating unnecessary columns
  
  votos_thresh<- votos_thresh%>%
    dplyr:: select(idleg, threshold)
  
  ### bringing back the threshold
  
  data_final <- merge(x=data, y=votos_thresh, by="idleg")
  
  data_final$distthresh <- (data_final$VOTOS/data_final$threshold)
  
  ### cutting the observations that are not in the threshold
  
  data_final_nele<-data_final%>%
    filter( !(resultado2=="Eleito") & (distthresh/0.5)>(1-y))
  
  data_final_ele<-data_final%>%
    filter( (resultado2=="Eleito") & (distthresh/0.5)<(1+y))
  
  data_final2<-rbind(data_final_ele,data_final_nele)
  
  data_final2<-data_final2%>%
    arrange(desc(idleg, VOTOS))
  
  ### resultados cortados
  return(data_final2) 
}

################
#4. Function Boas and Hidalgo - airwaves 2011
# This function generate datasets using the absolute votes as running variables
# The y is set as an amount of votes
# The function will select candidates with a vote difference from last elected at maximun y

threshold_bh <- function(data,y){
  
  #last elected
  
  data_ult_ele <- data %>%
    dplyr:: filter(flag==1)
  
  #first suplente
  data_pri_supl <-data%>%
    dplyr::filter(prim_supl==1)
  
  #only important columns
  
  votos_ult_ele <- data_ult_ele%>%
    dplyr:: select(idleg, VOTOS)
  
  votos_pri_supl <- data_pri_supl%>%
    dplyr:: select(idleg, VOTOS)
  
  ## merging
  
  votos_thresh <- merge(x=votos_ult_ele, y=votos_pri_supl, by="idleg")
  
  ## calculating threshold
  
  ### eliminating unnecessary columns
  
  votos_thresh<- votos_thresh%>%
    dplyr:: select(idleg, VOTOS.x, VOTOS.y)
  
  ### bringing back the threshold
  
  data_final <- merge(x=data, y=votos_thresh, by="idleg")
  
  ### selecing elected and not elected 
  
  data_final_nele<-data_final%>%
    filter( !(resultado2=="Eleito") )
  
  data_final_ele<-data_final%>%
    filter( (resultado2=="Eleito")  )
  
  
  ### calculating votes margin
  
  data_final_ele$distthresh <- (data_final_ele$VOTOS - data_final_ele$VOTOS.y)
  data_final_nele$distthresh<- (data_final_nele$VOTOS.x - data_final_nele$VOTOS)
  
  ### filtering
  
  data_final_ele <- data_final_ele %>%
    filter(distthresh<y)
  
  data_final_nele <- data_final_nele %>%
    filter(distthresh<y)
  
  ### consolidating
  
  data_final2<-rbind(data_final_ele,data_final_nele)
  
  data_final2<-data_final2%>%
    arrange(desc(idleg, VOTOS))
  
  # final results
  return(data_final2) 
}


#x <- threshold_media(data=distrital_dep_2014, y=0.15)
#x2 <- threshold_simples(data=distrital_dep_2014, y=0.15)
#x3 <- threshold_soma(data=distrital_dep_2014, y=0.15)
#x4<- threshold_bh(data=distrital_dep_2014, y=1000)


#5. Function State Share
# This function calculates the candidate vote share in their electoral district
# The share in the state is the running varible
# The distance from the shares between the candidate and the last elected and 
# first suplente will be used to cut the dataset

threshold_sharenom_uf <- function(data,y){
  
  ### calculo dos shares por ue
  
  data_votnom_ue <- data%>%
    group_by( SIGLA_UE)%>%
    summarise(VOTOS_UE = sum(VOTOS, na.rm=TRUE))
    
    
  ### merging dados da ue
    
    data_final <- merge(data, data_votnom_ue, by=c("SIGLA_UE"))
    
  ### Calculo do share nominal na ue
    
    data_final$share <- (data_final$VOTOS/data_final$VOTOS_UE)
    
    ### Calculo da distancia do share nominal
    
    #ultimo eleito
    
    data_ult_ele <- data_final %>%
      dplyr:: filter(flag==1)
    
    #primeiro suplente
    data_pri_supl <-data_final%>%
      dplyr::filter(prim_supl==1)
    
    ## s? colunas que importam
    
    share_ult_ele <- data_ult_ele%>%
      dplyr:: select(idleg, share)
    
    share_pri_supl <- data_pri_supl%>%
      dplyr:: select(idleg, share)
    
    ## merging
    
    share_thresh <- merge(x=share_ult_ele, y=share_pri_supl, by="idleg")
    
    ## calculating threshold
    
    ### eliminating unnecessary columns
    
    share_thresh<- share_thresh%>%
      dplyr:: select(idleg, share.x, share.y)
    
    ### bringing back the threshold
    
    data_final2 <- merge(x=data_final, y=share_thresh, by="idleg", all.x=TRUE)
    
    ### selecing elected and not elected 
    
    data_final_nele<-data_final2%>%
      filter( !(resultado2=="Eleito") )
    
    data_final_ele<-data_final2%>%
      filter( (resultado2=="Eleito")  )
    
    
    ### calculating votes margin
    
    data_final_ele$distthresh <- (data_final_ele$share - data_final_ele$share.y)
    data_final_nele$distthresh<- (data_final_nele$share.x - data_final_nele$share)
    
    ### filtering
    
    data_final_ele <- data_final_ele %>%
      filter(distthresh<y)
    
    data_final_nele <- data_final_nele %>%
      filter(distthresh<y)
    
    ### consolidating
    
    data_final2<-rbind(data_final_ele,data_final_nele)
    
    data_final2<-data_final2%>%
      arrange(desc(idleg, VOTOS))
    
  
  ### resultados cortados
  return(data_final2) 
}

x4<- threshold_sharenom_uf(data=distrital_dep_2014, y=0.01)

#5. Function Coaliton Share
# This function calculates the candidate vote share in their electoral Coalition
# The share in the coalition is the running varible
# The distance from the shares between the candidate and the last elected and 
# first suplente will be used to cut the dataset

threshold_sharenom_colig <- function(data,y){
  
  ### calculo dos shares por ue
  
  data_votnom_colig <- data%>%
    group_by( idleg)%>%
    summarise(VOTOS_COLIG = sum(VOTOS, na.rm=TRUE))
  
  
  ### merging dados da ue
  
  data_final <- merge(data, data_votnom_colig, by=c("idleg"))
  
  ### Calculo do share nominal na ue
  
  data_final$share <- (data_final$VOTOS/data_final$VOTOS_COLIG)
  
  ### Calculo da distancia do share nominal
  
  #last elected
  
  data_ult_ele <- data_final %>%
    dplyr:: filter(flag==1)
  
  #first suplente
  data_pri_supl <-data_final%>%
    dplyr::filter(prim_supl==1)
  
  ## s? colunas que importam
  
  share_ult_ele <- data_ult_ele%>%
    dplyr:: select(idleg, share)
  
  share_pri_supl <- data_pri_supl%>%
    dplyr:: select(idleg, share)
  
  ## merging
  
  share_thresh <- merge(x=share_ult_ele, y=share_pri_supl, by="idleg")
  
  ## calculating threshold
  
  ### eliminating unnecessary columns
  
  share_thresh<- share_thresh%>%
    dplyr:: select(idleg, share.x, share.y)
  
  ### bringing back the threshold
  
  data_final2 <- merge(x=data_final, y=share_thresh, by="idleg", all.x=TRUE)
  
  ### selecing elected and not elected 
  
  data_final_nele<-data_final2%>%
    filter( !(resultado2=="Eleito") )
  
  data_final_ele<-data_final2%>%
    filter( (resultado2=="Eleito")  )
  
  
  ### calculating votes margin
  
  data_final_ele$distthresh <- (data_final_ele$share - data_final_ele$share.y)
  data_final_nele$distthresh<- (data_final_nele$share.x - data_final_nele$share)
  
  ### filtering
  
  data_final_ele <- data_final_ele %>%
    filter(distthresh<y)
  
  data_final_nele <- data_final_nele %>%
    filter(distthresh<y)
  
  ### consolidating
  
  data_final2<-rbind(data_final_ele,data_final_nele)
  
  data_final2<-data_final2%>%
    arrange(desc(idleg, VOTOS))
  
  
  ### final results
  return(data_final2) 
}

x4<- threshold_sharenom_colig(data=distrital_dep_2014, y=0.10)


#7. Function Rank 
# This function generates the dataset using the candidate's rank as running variable

threshold_rank <- function(data,y){
  
  ### calculando distancia relativa
  
  data_ult_ele <- data %>%
    dplyr:: filter(flag==1)
  
  ## s? colunas que importam
  
  pos_ult_ele <- data_ult_ele%>%
    dplyr:: select(idleg, rank)
  
  ### merging de volta
  data_final <- merge(data, pos_ult_ele, by=c("idleg"), all.x=TRUE)
  
  ### vendo distancia
  data_final$dist_ultele<-data_final$rank.x - data_final$rank.y
  
  data_final$absdist_ultele<-abs(data_final$dist_ultele)
  
  ### filtering
  
  data_final <- data_final %>%
    filter(absdist_ultele<y)
  
  
  ### consolidating
  
  
  data_final<-data_final%>%
    arrange(desc(idleg, VOTOS))
  
  
  
  ### resultados cortados                           
  return(data_final) 
}


# TESTS
x4<- threshold_rank(data=distrital_dep_2014, y=4)
x5<- threshold_sharenom_colig(data=distrital_dep_part_2014, y=0.10)
x6<- threshold_sharenom_colig(data=distrital_dep_2014, y=0.10)