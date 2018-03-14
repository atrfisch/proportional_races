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


#Voting data Partidos
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
