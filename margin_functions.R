###################################################################
###################################################################
########  6. Functions to generate datasets with margin of victory
#0. Loading Data
#1. Function Percentage of last elected candidates 
#2. Function Distance of average of votes from last elected and first suplente
#3. Function Sum (IN DEVELOPMENT)
#4. Function Boas and Hidalgo (2011)
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
