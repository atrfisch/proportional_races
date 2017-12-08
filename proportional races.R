###################################################################
############# Downloading, Organizing and Cleaning Electoral Data
#0. Download TSE repositorio data
#1. Combine 
#2. Clean and get it ready for use
###################################################################
###################################################################

#Preambule
#R Version 3.3.2
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


################ Codes for nonregular elections

table(vot_1998$DESCRICAO_ELEICAO)
table(cand_1998$DESCRICAO_ELEICAO)

table(vot_2000$DESCRICAO_ELEICAO)
table(cand_2000$DESCRICAO_ELEICAO)

table(vot_2002$DESCRICAO_ELEICAO)
table(cand_2002$DESCRICAO_ELEICAO)

table(vot_2004$DESCRICAO_ELEICAO)
table(cand_2004$DESCRICAO_ELEICAO)

table(vot_2006$DESCRICAO_ELEICAO)
table(cand_2006$DESCRICAO_ELEICAO)

table(vot_2008$DESCRICAO_ELEICAO)
table(cand_2008$DESCRICAO_ELEICAO)

table(vot_2010$DESCRICAO_ELEICAO)
table(cand_2010$DESCRICAO_ELEICAO)

table(vot_2012$DESCRICAO_ELEICAO)
table(cand_2012$DESCRICAO_ELEICAO)

table(vot_2014$DESCRICAO_ELEICAO)
table(cand_2014$DESCRICAO_ELEICAO)

table(vot_2016$DESCRICAO_ELEICAO)
table(cand_2016$DESCRICAO_ELEICAO)

##########################################
########## MERGING #######################
##########################################
##########################################
##########################################

################# GENERAL ELECTIONS ###################


load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/vot_1998_2014.RData")
load("//fs-eesp-01/EESP/Usuarios/arthur.fisch/Dropbox/LOCAL_ELECTIONS/repositorio_data/original_unzipped/cand_1998_2014.RData")

#######Elections 2002 ###########


library(dplyr)

vot_2002 <- vot_1998_2014[[2]]
cand_2002 <- cand_1998_2014[[2]]

vot_2002<- vot_2002 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


#consolidating votes per candidate
cand_voto_02 <- vot_2002 %>%
  group_by(ANO_ELEICAO, NUM_TURNO, DESCRICAO_CARGO,CODIGO_CARGO, SEQUENCIAL_CANDIDATO,DESCRICAO_ELEICAO, SIGLA_UF,NUMERO_CAND,NOME_CANDIDATO,NOME_URNA_CANDIDATO, SEQUENCIAL_LEGENDA, SIGLA_PARTIDO)%>%
  summarise(VOTOS = sum(TOTAL_VOTOS))


#Merging
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

######
###### necessidade de filtrar pelos candidatos repeditos
######

#### FEDERAL DEPUTY ####

#filter to only Federal representatives

fed_dep_2002<- cand_2002v2 %>%
  filter(CODIGO_CARGO==6)


#ordering electoral coalitions

fed_dep_2002 <- mutate(fed_dep_2002, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

fed_dep_2002$idleg <-paste0(fed_dep_2002$SIGLA_UF,fed_dep_2002$CODIGO_CARGO, fed_dep_2002$sq_legenda2)

fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

fed_dep_2002 <-mutate(fed_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

fed_dep_2002$idleg2 <-paste0(fed_dep_2002$idleg, fed_dep_2002$resultado2)

fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

fed_dep_2002 <-mutate(fed_dep_2002, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

fed_dep_2002 <- fed_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))


#### STATE DEPUTY ####

#filter to only Federal representatives

state_dep_2002<- cand_2002v2 %>%
  filter(CODIGO_CARGO==7)


#ordering electoral coalitions

state_dep_2002 <- mutate(state_dep_2002, sq_legenda2 = ifelse(SEQUENCIAL_LEGENDA==-3, as.character(SIGLA_PARTIDO.x), ifelse(SEQUENCIAL_LEGENDA==-1, as.character(SIGLA_PARTIDO.x),as.numeric(SEQUENCIAL_LEGENDA))))

state_dep_2002$idleg <-paste0(state_dep_2002$SIGLA_UF,state_dep_2002$CODIGO_CARGO, state_dep_2002$sq_legenda2)

state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg, desc(VOTOS)) %>%
  group_by(idleg) %>% 
  mutate(rank = rank(idleg, ties.method = "first"))

state_dep_2002 <-mutate(state_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

state_dep_2002$idleg2 <-paste0(state_dep_2002$idleg, state_dep_2002$resultado2)

state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(rank2 = rank(c(idleg2), ties.method = "first"))

state_dep_2002 <-mutate(state_dep_2002, prim_supl = ifelse((rank2==1 & resultado2=="Suplente"),1,0))

state_dep_2002 <- state_dep_2002 %>%
  arrange (idleg2, desc(VOTOS)) %>%
  group_by(idleg2) %>% 
  mutate(flag = ifelse( (rank(c(idleg2), ties.method = "last")==1 & resultado2=="Eleito"),1,0))



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

distrital_dep_2002 <-mutate(distrital_dep_2002, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito", ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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


#######Elections 2006 ###########


library(dplyr)

vot_2006 <- vot_1998_2014[[3]]
cand_2006 <- cand_1998_2014[[3]]

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

######
###### necessidade de filtrar pelos candidatos repeditos
######

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

fed_dep_2006 <-mutate(fed_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

state_dep_2006 <-mutate(state_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

distrital_dep_2006 <-mutate(distrital_dep_2006, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))


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

#######Elections 2010 ###########


library(dplyr)

vot_2010 <- vot_1998_2014[[4]]
cand_2010 <- cand_1998_2014[[4]]

vot_2014<- vot_2014 %>% rename(SEQUENCIAL_CANDIDATO = SQ_CANDIDATO)


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

######
###### necessidade de filtrar pelos candidatos repeditos
######

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

fed_dep_2010 <-mutate(fed_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

state_dep_2010 <-mutate(state_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

distrital_dep_2010 <-mutate(distrital_dep_2010, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO","Eleito",ifelse(DESC_SIT_TOT_TURNO=="MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))


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

#######Elections 2014 ###########


library(dplyr)

vot_2014 <- vot_1998_2014[[5]]
cand_2014 <- cand_1998_2014[[5]]

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

######
###### necessidade de filtrar pelos candidatos repeditos
######

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

fed_dep_2014 <-mutate(fed_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

state_dep_2014 <-mutate(state_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))

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

distrital_dep_2014 <-mutate(distrital_dep_2014, resultado2 = ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR QP","Eleito",ifelse(DESC_SIT_TOT_TURNO=="ELEITO POR MÉDIA","Eleito",ifelse(DESC_SIT_TOT_TURNO=="SUPLENTE","Suplente","Não eleito"))))


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
