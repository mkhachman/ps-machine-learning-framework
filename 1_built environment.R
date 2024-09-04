rm(list=ls())
gc()
options(scipen=100,digits=22)
library(data.table)
library(dplyr)
library(stringr)


###par
par<-fread("Input/PAR/par20.csv")
#filter par to mtl municipalities
mun_mtl<-c('66007','66023','66032','66047','66058','66062','66072','66087','66092','66097','66102','66107','66112','66117','66127','66142')
par<-par[par$code_mun%in%mun_mtl]
par$code_mun<-as.numeric(par$code_mun)
par[is.na(par)]<-0 #replace NAs with 0
#rename
par<-data.table(par_id=par$id_provinc,
                     par_use=par$code_utilisation,
                     par_mun=as.numeric(par$code_mun),
                     par_landwidth=par$land_width_m,
                     par_landarea=par$land_area_sq_m,
                     par_floors=par$building_levels,
                     par_consyr=par$building_start_date,
                     par_consyrest=par$building_start_date_is_estimated,
                     par_bldarea=par$building_levels_area_sq_m,
                     par_type=par$code_lien_physique,
                     par_flats=par$building_flats,
                     par_nonflats=par$building_non_flats_spaces,
                     par_rentalrooms=par$building_rental_rooms,
                     par_landvalue=as.numeric(par$value_land),
                     par_bldvalue=as.numeric(par$value_building),
                     par_totvalue=as.numeric(par$value_total),
                     par_lon=par$X,
                     par_lat=par$Y,
                     par_location=paste(as.character.numeric_version(par$X),as.character.numeric_version(par$Y),sep=";")
)
#location id
temp<-data.table(par_locationid=1:length(unique(par$par_location)),par_location=unique(par$par_location))
par<-left_join(par,temp,by=c("par_location"="par_location"))
write.csv(par,"Input/PAR/par.csv")
temp<-unique(par[,c("par_locationid","par_lon","par_lat")])
write.csv(temp,"Input/PAR/par_loc.csv")


###pau
pau<-fread("Input/PAU/pau22.csv")
#rename
pau<-data.table(pau_id=pau$ID_UEV,
                pau_from=pau$CIVIQUE_DE,
                pau_suffixfrom=pau$LETTRE_DEB,
                pau_to=pau$CIVIQUE_FI,
                pau_suffixto=pau$LETTRE_FIN,
                pau_street=pau$NOM_RUE,
                pau_unit=pau$SUITE_DEBU,
                pau_mun=pau$MUNICIPALI,
                pau_floors=pau$ETAGE_HORS,
                pau_ducount=pau$NOMBRE_LOG,
                pau_consyr=pau$ANNEE_CONS,
                pau_usagecode=pau$CODE_UTILI,
                pau_usage=pau$LIBELLE_UT,
                pau_dutype=pau$CATEGORIE_,
                pau_landarea=pau$SUPERFICIE,
                pau_bldarea=pau$SUPERFIC_1,
                pau_borough=pau$NO_ARROND_,
                pau_geo=pau$WKT)
#geometry id
temp<-data.table(pau_geoid=1:length(unique(pau$pau_geo)),pau_geo=unique(pau$pau_geo))
pau<-left_join(pau,temp,by=c("pau_geo"="pau_geo"))
write.csv(pau,"Input/PAU/pau.csv")
temp<-unique(pau[,c("pau_geoid","pau_geo")])
write.csv(temp,"Input/PAU/pau_geo.csv")


###addresses
add<-fread("Input/Addresses/add22.csv")
#Rename
names(add)<-c("add_id","add_text","add_specific","add_orientation","add_link","add_height","add_generic","add_angle","add_from","add_to","add_x","add_y","add_lon","add_lat")
#Location id
add$add_location<-paste(add$add_x,add$add_y,sep=";")
temp<-data.table(add_locationid=1:length(unique(add$add_location)),add_location=unique(add$add_location))
add<-left_join(add,temp,by=c("add_location"="add_location"))
write.csv(add,"Input/Addresses/add.csv")
temp<-unique(add[,c("add_locationid","add_x","add_y")])
write.csv(temp,"Input/Addresses/add_loc.csv")


###import pre-processed databases
add<-fread("Input/Addresses/add.csv")
par<-fread("Input/PAR/par.csv")
pau<-fread("Input/PAU/pau.csv")
add<-add[,!"V1"]
par<-par[,!"V1"]
pau<-pau[,!"V1"]
pau[is.na(pau),]<-0
#pau_add
pau_add<-fread("Input/PAU/pau_add.csv")
pau_add<-pau_add[,!"field_1"]
names(pau_add)[ncol(pau_add)]<-"add_locationid"
pau_add_near<-fread("Input/PAU/paugeo_wt_addloc_near_addloc.csv")
pau_add_near<-pau_add_near[,c("pau_geoid","add_locati")]
pau_add$add_locationid[is.na(pau_add$add_locationid)]<-pau_add_near$add_locati
pau_add<-left_join(pau_add,pau[,c("pau_id","pau_geoid")])
#correcting special characters
add$add_generic<-gsub("Ã¢","a",add$add_generic)
add$add_generic<-gsub("Ã§","c",add$add_generic)
add$add_generic<-gsub("Ã‰","e",add$add_generic)
add$add_generic<-gsub("Ã©","e",add$add_generic)
add$add_generic<-gsub("Ã€°","e",add$add_generic)
add$add_generic<-gsub("Ã¨","e",add$add_generic)
add$add_generic<-gsub("Ãª","e",add$add_generic)
add$add_generic<-gsub("Ã«","e",add$add_generic)
add$add_generic<-gsub("Ãˆ","e",add$add_generic)
add$add_generic<-gsub("Ã®","i",add$add_generic)
add$add_generic<-gsub("Ã¯","i",add$add_generic)
add$add_generic<-gsub("ÃŽ","i",add$add_generic)
add$add_generic<-gsub("Ã´","o",add$add_generic)
add$add_generic<-gsub("Ã»","u",add$add_generic)
add$add_generic<-gsub("Ã","a",add$add_generic)
add$add_specific<-gsub("Ã¢","a",add$add_specific)
add$add_specific<-gsub("Ã§","c",add$add_specific)
add$add_specific<-gsub("Ã‰","e",add$add_specific)
add$add_specific<-gsub("Ã©","e",add$add_specific)
add$add_specific<-gsub("Ã€°","e",add$add_specific)
add$add_specific<-gsub("Ã¨","e",add$add_specific)
add$add_specific<-gsub("Ãª","e",add$add_specific)
add$add_specific<-gsub("Ã«","e",add$add_specific)
add$add_specific<-gsub("Ãˆ","e",add$add_specific)
add$add_specific<-gsub("Ã®","i",add$add_specific)
add$add_specific<-gsub("Ã¯","i",add$add_specific)
add$add_specific<-gsub("ÃŽ","i",add$add_specific)
add$add_specific<-gsub("Ã´","o",add$add_specific)
add$add_specific<-gsub("Ã»","u",add$add_specific)
add$add_specific<-gsub("Ã","a",add$add_specific)
pau$pau_street<-gsub("Ã¢","a",pau$pau_street)
pau$pau_street<-gsub("Ã§","c",pau$pau_street)
pau$pau_street<-gsub("Ã‰","e",pau$pau_street)
pau$pau_street<-gsub("Ã©","e",pau$pau_street)
pau$pau_street<-gsub("Ã€°","e",pau$pau_street)
pau$pau_street<-gsub("Ã¨","e",pau$pau_street)
pau$pau_street<-gsub("Ãª","e",pau$pau_street)
pau$pau_street<-gsub("Ã«","e",pau$pau_street)
pau$pau_street<-gsub("Ãˆ","e",pau$pau_street)
pau$pau_street<-gsub("Ã®","i",pau$pau_street)
pau$pau_street<-gsub("Ã¯","i",pau$pau_street)
pau$pau_street<-gsub("ÃŽ","i",pau$pau_street)
pau$pau_street<-gsub("Ã´","o",pau$pau_street)
pau$pau_street<-gsub("Ã»","u",pau$pau_street)
pau$pau_street<-gsub("Ã","a",pau$pau_street)
pau$pau_usage<-gsub("Ã¢","a",pau$pau_usage)
pau$pau_usage<-gsub("Ã§","c",pau$pau_usage)
pau$pau_usage<-gsub("Ã‰","e",pau$pau_usage)
pau$pau_usage<-gsub("Ã©","e",pau$pau_usage)
pau$pau_usage<-gsub("Ã€°","e",pau$pau_usage)
pau$pau_usage<-gsub("Ã¨","e",pau$pau_usage)
pau$pau_usage<-gsub("Ãª","e",pau$pau_usage)
pau$pau_usage<-gsub("Ã«","e",pau$pau_usage)
pau$pau_usage<-gsub("Ãˆ","e",pau$pau_usage)
pau$pau_usage<-gsub("Ã®","i",pau$pau_usage)
pau$pau_usage<-gsub("Ã¯","i",pau$pau_usage)
pau$pau_usage<-gsub("ÃŽ","i",pau$pau_usage)
pau$pau_usage<-gsub("Ã´","o",pau$pau_usage)
pau$pau_usage<-gsub("Ã»","u",pau$pau_usage)
pau$pau_usage<-gsub("Ã","a",pau$pau_usage)
pau$pau_dutype<-gsub("Ã¢","a",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã§","c",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã‰","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã©","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã€°","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã¨","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ãª","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã«","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ãˆ","e",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã®","i",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã¯","i",pau$pau_dutype)
pau$pau_dutype<-gsub("ÃŽ","i",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã´","o",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã»","u",pau$pau_dutype)
pau$pau_dutype<-gsub("Ã","a",pau$pau_dutype)
add$add_orientation[add$add_orientation=="E"]<-"est"
add$add_orientation[add$add_orientation=="O"]<-"ouest"
add$add_orientation[add$add_orientation=="N"]<-"nord"
add$add_orientation[add$add_orientation=="S"]<-"sud"
add$add_specific<-str_to_lower(add$add_specific)
add$add_generic<-str_to_lower(add$add_generic)
pau$pau_street<-str_to_lower(pau$pau_street)
add$add_specific[add$add_specific=="upper-belmont"]<-"upper belmont"


###temp files
#temp_add
add$add_unique<-paste(add$add_locationid,add$add_generic,add$add_specific,add$add_orientation)
agg<-aggregate(data=add,add_from~add_unique,min)
names(agg)[ncol(agg)]<-"add_minfrom"
add<-left_join(add,agg)
agg<-aggregate(data=add,add_to~add_unique,max)
names(agg)[ncol(agg)]<-"add_maxto"
add<-left_join(add,agg)
add$add_mfodd<-add$add_minfrom%%2
add$add_mtodd<-add$add_maxto%%2
temp_add<-unique(add[,c("add_locationid","add_minfrom","add_maxto","add_mfodd","add_mtodd","add_generic","add_specific","add_orientation")])
temp_add$add_orientation[temp_add$add_orientation=="X"]<-""
temp_add$add_orientation[temp_add$add_orientation!=""]<-paste(" ",temp_add$add_orientation[temp_add$add_orientation!=""],sep="")
#temp_pau
temp_pau<-pau[,c("pau_id","pau_from","pau_to","pau_street")]
temp_pau$pau_street<-gsub(r"{\s*\([^\)]+\)}","",temp_pau$pau_street)


###associate each pau_id with a single add_locationid
fun<-function(x, y){
  grepl(x, y)
}
#intersection & generic & specific & orientation & from-to (add_location englobing pau)
temp<-left_join(temp_pau,pau_add[,c("pau_id","add_locationid")])
temp<-left_join(temp,temp_add)
temp<-temp[mapply(fun,paste("^",temp$add_specific,"|",temp$add_specific,"$","|"," ",temp$add_specific," ",sep=""),temp$pau_street)
         & temp$pau_from>=temp$add_minfrom & temp$pau_to<=temp$add_maxto
         & mapply(fun,temp$add_orientation,temp$pau_street)
         & mapply(fun,temp$add_generic,temp$pau_street)]
temp1<-temp[temp$pau_id%in%as.numeric(names(table(temp$pau_id)[table(temp$pau_id)>1]))]
for (i in 1:length(unique(temp1$pau_id))){
  print(i)
  temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]][sample(1:length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]]),(length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]])-1),replace=FALSE)]<-0
}
temp<-temp[temp$add_locationid!=0]
pau_res<-arrange(temp,temp$pau_id)
temp_pau<-temp_pau[!temp_pau$pau_id%in%pau_res$pau_id]
#intersection & generic & specific & orientation & from-to (pau englobing add_location)
temp<-left_join(temp_pau,pau_add[,c("pau_id","add_locationid")])
temp<-left_join(temp,temp_add)
temp<-temp[mapply(fun,paste("^",temp$add_specific,"|",temp$add_specific,"$","|"," ",temp$add_specific," ",sep=""),temp$pau_street)
           & temp$pau_from<=temp$add_minfrom & temp$pau_to>=temp$add_maxto
           & mapply(fun,temp$add_orientation,temp$pau_street)
           & mapply(fun,temp$add_generic,temp$pau_street)]
temp1<-temp[temp$pau_id%in%as.numeric(names(table(temp$pau_id)[table(temp$pau_id)>1]))]
for (i in 1:length(unique(temp1$pau_id))){
  print(i)
  temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]][sample(1:length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]]),(length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]])-1),replace=FALSE)]<-0
}
temp<-temp[temp$add_locationid!=0]
pau_res<-rbind(pau_res,temp)
pau_res<-arrange(pau_res,pau_res$pau_id)
temp_pau<-temp_pau[!temp_pau$pau_id%in%pau_res$pau_id]
#intersection & specific & orientation & from-to (add_location englobing pau) : generic relaxed
temp<-left_join(temp_pau,pau_add[,c("pau_id","add_locationid")])
temp<-left_join(temp,temp_add)
temp<-temp[mapply(fun,paste("^",temp$add_specific,"|",temp$add_specific,"$","|"," ",temp$add_specific," ",sep=""),temp$pau_street)
           & temp$pau_from>=temp$add_minfrom & temp$pau_to<=temp$add_maxto
           & mapply(fun,temp$add_orientation,temp$pau_street)]
temp1<-temp[temp$pau_id%in%as.numeric(names(table(temp$pau_id)[table(temp$pau_id)>1]))]
for (i in 1:length(unique(temp1$pau_id))){
  print(i)
  temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]][sample(1:length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]]),(length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]])-1),replace=FALSE)]<-0
}
temp<-temp[temp$add_locationid!=0]
pau_res<-rbind(pau_res,temp)
pau_res<-arrange(pau_res,pau_res$pau_id)
temp_pau<-temp_pau[!temp_pau$pau_id%in%pau_res$pau_id]
#intersection & generic & specific & orientation & from-to (pau 99999)
temp<-left_join(temp_pau,pau_add[,c("pau_id","add_locationid")])
temp<-left_join(temp,temp_add)
temp<-temp[mapply(fun,paste("^",temp$add_specific,"|",temp$add_specific,"$","|"," ",temp$add_specific," ",sep=""),temp$pau_street)
     & temp$pau_from==99999 & temp$pau_to==99999
     & mapply(fun,temp$add_orientation,temp$pau_street)
     & mapply(fun,temp$add_generic,temp$pau_street)]
temp1<-temp[temp$pau_id%in%as.numeric(names(table(temp$pau_id)[table(temp$pau_id)>1]))]
for (i in 1:length(unique(temp1$pau_id))){
  print(i)
  temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]][sample(1:length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]]),(length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]])-1),replace=FALSE)]<-0
}
temp<-temp[temp$add_locationid!=0]
pau_res<-rbind(pau_res,temp)
pau_res<-arrange(pau_res,pau_res$pau_id)
temp_pau<-temp_pau[!temp_pau$pau_id%in%pau_res$pau_id]
#intersection & generic & specific & orientation & from-to (pau 99999) : generic relaxed
temp<-left_join(temp_pau,pau_add[,c("pau_id","add_locationid")])
temp<-left_join(temp,temp_add)
temp[mapply(fun,paste("^",temp$add_specific,"|",temp$add_specific,"$","|"," ",temp$add_specific," ",sep=""),temp$pau_street)
           & temp$pau_from==99999 & temp$pau_to==99999
           & mapply(fun,temp$add_orientation,temp$pau_street)]
temp1<-temp[temp$pau_id%in%as.numeric(names(table(temp$pau_id)[table(temp$pau_id)>1]))]
for (i in 1:length(unique(temp1$pau_id))){
  print(i)
  temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]][sample(1:length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]]),(length(temp$add_locationid[temp$pau_id==unique(temp1$pau_id)[i]])-1),replace=FALSE)]<-0
}
temp<-temp[temp$add_locationid!=0]
pau_res<-rbind(pau_res,temp)
pau_res<-arrange(pau_res,pau_res$pau_id)
temp_pau<-temp_pau[!temp_pau$pau_id%in%pau_res$pau_id]
pau<-left_join(pau,pau_res[,c("pau_id","add_locationid")])


###pau_par
pau$pau_consyr[pau$pau_consyr==9999]<-0
pau_par<-fread("Input/PAU/pau_par.csv")
pau_par<-pau_par[,!"field_1"]
names(pau_par)[ncol(pau_par)]<-"par_locationid"
#remove pau_geoids that do not intersect a par and have 0 dwellings units
agg<-data.table(aggregate(data=pau,pau_ducount~pau_geoid,sum))
pau<-pau[!(pau$pau_geoid%in%pau_par$pau_geoid[is.na(pau_par$par_locationid)] & pau$pau_geoid%in%agg$pau_geoid[agg$pau_ducount==0])]
pau_par<-pau_par[pau_par$pau_geoid%in%pau$pau_geoid]
#remove pau_geoids that do not intersect a par and are built after 2019 (which is the maximun construction year for par)
pau<-pau[!(pau$pau_geoid%in%pau_par$pau_geoid[is.na(pau_par$par_locationid)] & pau$pau_geoid%in%unique(pau$pau_geoid[pau$pau_consyr>max(par$par_consyr)]))]
pau_par<-pau_par[pau_par$pau_geoid%in%pau$pau_geoid]
#par_pau
par_pau<-fread("Input/PAR/par_pau.csv")
par_pau<-par_pau[,!"field_1"]
par_pau<-par_pau[,!c("par_lon","par_lat")]
names(par_pau)[1]<-"par_locationid"
#remove par_locationids that do not intersect a pau and have 0 flats
agg<-data.table(aggregate(data=par,par_flats~par_locationid,sum))
par<-par[!(par$par_locationid%in%par_pau$par_locationid[is.na(par_pau$pau_geoid)] & par$par_locationid%in%agg$par_locationid[agg$par_flats==0])]
par_pau<-par_pau[par_pau$par_locationid%in%par$par_locationid]
#par_wt_pau<-left_join(par_pau[is.na(par_pau$pau_geoid)],unique(par[,c("par_locationid","par_lon","par_lat")]))[,c("par_locationid","par_lon","par_lat")]
#write.csv(par_wt_pau,"Input/PAR/par_wt_pau.csv")
#pau_wt_par<-left_join(pau_par[is.na(pau_par$par_locationid)],unique(pau[,c("pau_geoid","pau_geo")]))[,c("pau_geoid","pau_geo")]
#write.csv(pau_wt_par,"Input/PAU/pau_wt_par.csv")
#qgis : join pau_wt_par to nearest par_wt_pau within 100m
pauwtpar_parwtpau<-fread("Input/PAU/pauwtpar_parwtpau_100.csv")
pauwtpar_parwtpau<-pauwtpar_parwtpau[!is.na(pauwtpar_parwtpau$par_locationid)]
names(pauwtpar_parwtpau)[names(pauwtpar_parwtpau)=="par_locationid"]<-"par_locationid2"
pau_par<-left_join(pau_par,pauwtpar_parwtpau[,c("pau_geoid","par_locationid2")])
pau_par$par_locationid[!is.na(pau_par$par_locationid2)]<-pau_par$par_locationid2[!is.na(pau_par$par_locationid2)]
pau_par<-pau_par[,!"par_locationid2"]
#qgis : join pau_wt_par to nearest par within 100m
pauwtpar_par<-fread("Input/PAU/pauwtpar_par_100.csv")
pauwtpar_par<-pauwtpar_par[!is.na(pauwtpar_par$par_locati)]
pau_par<-left_join(pau_par,pauwtpar_par[,c("pau_geoid","par_locati")])
pau_par$par_locationid[!is.na(pau_par$par_locati)]<-pau_par$par_locati[!is.na(pau_par$par_locati)]
pau_par<-pau_par[,!"par_locati"]
#filter to paus with dwelling units built before 2019 (as depending on the construction month some units built on 2019 may be lacking from the PAR)
pau<-pau[!(pau$pau_geoid%in%pau_par$pau_geoid[is.na(pau_par$par_locationid)] & pau$pau_geoid%in%unique(pau$pau_geoid[pau$pau_consyr>=max(par$par_consyr)]))]
pau_par<-pau_par[pau_par$pau_geoid%in%pau$pau_geoid]
pau<-left_join(pau,pau_par)


###bld
bld<-left_join(pau[,c("add_locationid","pau_geoid","pau_id","pau_ducount","pau_floors","pau_consyr","pau_landarea","pau_bldarea","pau_usagecode","pau_usage","pau_dutype","par_locationid")],par[,c("par_locationid","par_id","par_flats","par_floors","par_consyr","par_landarea","par_bldarea","par_use","par_type")])
agg<-data.table(aggregate(data=bld,add_locationid~par_id,function(x) length(unique(x))))
names(agg)[ncol(agg)]<-"par_naddl"
bld_res<-bld[bld$par_id%in%agg$par_id[agg$par_naddl==1]]
bld_rem<-bld[!bld$par_id%in%bld_res$par_id]
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_ducount==par_flats
a<-unique(bld_rem$par_id[bld_rem$pau_ducount==bld_rem$par_flats])
b<-unique(bld_rem$par_id[bld_rem$pau_ducount!=bld_rem$par_flats])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$pau_ducount!=bld_rem$par_flats)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_usagecode==par_use
a<-unique(bld_rem$par_id[bld_rem$pau_usagecode==bld_rem$par_use])
b<-unique(bld_rem$par_id[bld_rem$pau_usagecode!=bld_rem$par_use])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$pau_usagecode!=bld_rem$par_use)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_consyr==par_consyr
a<-unique(bld_rem$par_id[bld_rem$pau_consyr==bld_rem$par_consyr])
b<-unique(bld_rem$par_id[bld_rem$pau_consyr!=bld_rem$par_consyr])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$pau_consyr!=bld_rem$par_consyr)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_bldarea==round(par_bldarea,0)
a<-unique(bld_rem$par_id[bld_rem$pau_bldarea==round(bld_rem$par_bldarea,0)])
b<-unique(bld_rem$par_id[!(bld_rem$pau_bldarea==round(bld_rem$par_bldarea,0))])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !(bld_rem$pau_bldarea==round(bld_rem$par_bldarea,0)))]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_bldarea==floor/ceiling(par_bldarea)
a<-unique(bld_rem$par_id[bld_rem$pau_bldarea==floor(bld_rem$par_bldarea) | bld_rem$pau_bldarea==ceiling(bld_rem$par_bldarea)])
b<-unique(bld_rem$par_id[!(bld_rem$pau_bldarea==floor(bld_rem$par_bldarea) | bld_rem$pau_bldarea==ceiling(bld_rem$par_bldarea))])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !(bld_rem$pau_bldarea==floor(bld_rem$par_bldarea) | bld_rem$pau_bldarea==ceiling(bld_rem$par_bldarea)))]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_landarea==round(par_landarea,0)
a<-unique(bld_rem$par_id[bld_rem$pau_landarea==round(bld_rem$par_landarea,0)])
b<-unique(bld_rem$par_id[!(bld_rem$pau_landarea==round(bld_rem$par_landarea,0))])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !(bld_rem$pau_landarea==round(bld_rem$par_landarea,0)))]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_landarea==floor/ceiling(par_landarea)
a<-unique(bld_rem$par_id[bld_rem$pau_landarea==floor(bld_rem$par_landarea) | bld_rem$pau_landarea==ceiling(bld_rem$par_landarea)])
b<-unique(bld_rem$par_id[!(bld_rem$pau_landarea==floor(bld_rem$par_landarea) | bld_rem$pau_landarea==ceiling(bld_rem$par_landarea))])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !(bld_rem$pau_landarea==floor(bld_rem$par_landarea) | bld_rem$pau_landarea==ceiling(bld_rem$par_landarea)))]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##pau_floors==par_floors
a<-unique(bld_rem$par_id[bld_rem$pau_floors==bld_rem$par_floors])
b<-unique(bld_rem$par_id[bld_rem$pau_floors!=bld_rem$par_floors])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$pau_floors!=bld_rem$par_floors)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##delta_bldarea<=1.1*min_delta_bldarea
bld_rem$delta_bldarea<-abs(bld_rem$pau_bldarea-bld_rem$par_bldarea)
agg<-aggregate(data=bld_rem,delta_bldarea~par_id,min)
names(agg)[ncol(agg)]<-"min_delta_bldarea"
bld_rem<-left_join(bld_rem,agg)
a<-unique(bld_rem$par_id[bld_rem$delta_bldarea<=1.1*bld_rem$min_delta_bldarea])
b<-unique(bld_rem$par_id[bld_rem$delta_bldarea>1.1*bld_rem$min_delta_bldarea])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$delta_bldarea>1.1*bld_rem$min_delta_bldarea)]
bld_rem<-bld_rem[,!c("delta_bldarea","min_delta_bldarea")]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##delta_landarea<=1.1*min_delta_landarea
bld_rem$delta_landarea<-abs(bld_rem$pau_landarea-bld_rem$par_landarea)
agg<-aggregate(data=bld_rem,delta_landarea~par_id,min)
names(agg)[ncol(agg)]<-"min_delta_landarea"
bld_rem<-left_join(bld_rem,agg)
a<-unique(bld_rem$par_id[bld_rem$delta_landarea<=1.1*bld_rem$min_delta_landarea])
b<-unique(bld_rem$par_id[bld_rem$delta_landarea>1.1*bld_rem$min_delta_landarea])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$delta_landarea>1.1*bld_rem$min_delta_landarea)]
bld_rem<-bld_rem[,!c("delta_landarea","min_delta_landarea")]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##delta_totarea<=1.1*min_delta_totarea
bld_rem$pau_totarea<-bld_rem$pau_landarea+bld_rem$pau_bldarea
bld_rem$par_totarea<-bld_rem$par_landarea+bld_rem$par_bldarea
bld_rem$delta_totarea<-abs(bld_rem$pau_totarea-bld_rem$par_totarea)
agg<-aggregate(data=bld_rem,delta_totarea~par_id,min)
names(agg)[ncol(agg)]<-"min_delta_totarea"
bld_rem<-left_join(bld_rem,agg)
a<-unique(bld_rem$par_id[bld_rem$delta_totarea<=1.1*bld_rem$min_delta_totarea])
b<-unique(bld_rem$par_id[bld_rem$delta_totarea>1.1*bld_rem$min_delta_totarea])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & bld_rem$delta_totarea>1.1*bld_rem$min_delta_totarea)]
bld_rem<-bld_rem[,!c("pau_totarea","par_totarea","delta_totarea","min_delta_totarea")]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}
#filter bld_rem to add_locations lacking dus only
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
temp<-agg$add_locationid[agg$pau_ducount_res<agg$pau_ducount]
a<-unique(bld_rem$par_id[bld_rem$add_locationid%in%temp])
b<-unique(bld_rem$par_id[!bld_rem$add_locationid%in%temp])
int<-a[a%in%b]
bld_rem<-bld_rem[!(bld_rem$par_id%in%int & !bld_rem$add_locationid%in%temp)]
#update bld_res and bld_rem
a<-0
i<-1
while(nrow(bld_rem)!=a){
  print(i)
  a<-nrow(bld_rem)
  #par_naddl==1
  agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  #par_naddl[addl_npar==1]==1
  temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
  agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
  temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
  agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
  names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
  agg<-left_join(agg,agg_res)
  agg[is.na(agg)]<-0
  temp<-agg$add_locationid[agg$pau_ducount==0 | agg$pau_ducount_res<agg$pau_ducount]
  agg<-data.table(aggregate(data=bld_rem,par_id~add_locationid,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"addl_npar"
  temp<-bld_rem[(bld_rem$add_locationid%in%temp) & bld_rem$add_locationid%in%agg$add_locationid[agg$addl_npar==1]]
  agg<-data.table(aggregate(data=temp,add_locationid~par_id,function(x) length(unique(x))))
  names(agg)[ncol(agg)]<-"par_naddl"
  bld_res<-rbind(bld_res,bld_rem[(bld_rem$add_locationid%in%temp$add_locationid) & bld_rem$par_id%in%agg$par_id[agg$par_naddl==1]])
  bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id]
  i<-i+1
}

##number of remaining par==number of corresponding add_locations & pars<=>add_locations : randomly assign one par to each add_location
agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,unique))
agg$addlcomb<-as.character(agg$add_locationid)
temp<-data.table(addlcomb=unique(as.character(agg$add_locationid)))
temp<-data.table(addlcomb_id=1:nrow(temp),temp)
temp<-left_join(agg,temp)
agg<-aggregate(data=temp,par_id~addlcomb_id,function(x) length(unique(x)))
names(agg)[ncol(agg)]<-"addlcomb_npar"
temp<-left_join(temp,agg)
temp$test<-NA
temp$addlcomb_length<-NA
for(i in 1:nrow(temp)){
  print(i)
  temp$test[i]<-length(unlist(temp$add_locationid)[unlist(temp$add_locationid)%in%unlist(temp$add_locationid[i])])/length(unlist(temp$add_locationid[i]))
  temp$addlcomb_length[i]<-length(unique(unlist(temp$add_locationid[i])))
}
temp<-temp[temp$addlcomb_npar==temp$test]
temp_rem<-temp[temp$addlcomb_length==temp$addlcomb_npar]
keep<-data.table()
for (i in 1:nrow(temp_rem)){
  print(i)
  keep<-rbind(keep,bld_rem[bld_rem$par_id==temp_rem$par_id[i]][sample(1:nrow(bld_rem[bld_rem$par_id==temp_rem$par_id[i]]),1,replace=FALSE),])
  bld_rem<-bld_rem[!(bld_rem$par_id%in%keep$par_id | bld_rem$add_locationid%in%keep$add_locationid)]
}
bld_res<-rbind(bld_res,keep)

##number of remaining par!=number of corresponding add_locations & pars<=>add_locations : assign iteratively one par to each add_location until its pau_ducount is met
agg<-data.table(aggregate(data=bld_rem,add_locationid~par_id,unique))
agg$addlcomb<-as.character(agg$add_locationid)
temp<-data.table(addlcomb=unique(as.character(agg$add_locationid)))
temp<-data.table(addlcomb_id=1:nrow(temp),temp)
temp<-left_join(agg,temp)
agg<-aggregate(data=temp,par_id~addlcomb_id,function(x) length(unique(x)))
names(agg)[ncol(agg)]<-"addlcomb_npar"
temp<-left_join(temp,agg)
temp$test<-NA
temp$addlcomb_length<-NA
for(i in 1:nrow(temp)){
  print(i)
  temp$test[i]<-length(unlist(temp$add_locationid)[unlist(temp$add_locationid)%in%unlist(temp$add_locationid[i])])/length(unlist(temp$add_locationid[i]))
  temp$addlcomb_length[i]<-length(unique(unlist(temp$add_locationid[i])))
}
temp_rem<-temp[temp$addlcomb_npar==temp$test]
temp_rem_exp<-unique(bld_rem[bld_rem$par_id%in%temp_rem$par_id,c("par_id","add_locationid")])
agg<-aggregate(data=temp_rem_exp,add_locationid~par_id,unique)
names(agg)[ncol(agg)]<-"addlcomb"
agg$addlcomb<-as.character(agg$addlcomb)
temp_rem_exp<-left_join(temp_rem_exp,agg)
temp_rem_exp<-unique(left_join(temp_rem_exp,temp_rem[,c("addlcomb","addlcomb_id")])[,!"addlcomb"])
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
agg$delta_pauducount<-agg$pau_ducount-agg$pau_ducount_res
temp<-agg
temp_rem_exp<-left_join(temp_rem_exp,temp[,c("add_locationid","pau_ducount","delta_pauducount")])
temp_rem_exp<-left_join(temp_rem_exp,unique(temp_rem[,c("addlcomb_id","addlcomb_npar","addlcomb_length")]))
agg<-data.table(aggregate(data=unique(temp_rem_exp[,c("addlcomb_id","pau_ducount")]),pau_ducount~addlcomb_id,sum))
names(agg)[ncol(agg)]<-"addlcomb_pauducount"
temp_rem_exp<-left_join(temp_rem_exp,agg)
bld_rem<-unique(left_join(bld_rem,temp_rem_exp[,c("add_locationid","addlcomb_id","addlcomb_npar","addlcomb_length","delta_pauducount","addlcomb_pauducount")],by=c("add_locationid"="add_locationid")))
#pau_ducount==0
a<-bld_rem[bld_rem$par_id%in%temp_rem_exp$par_id & bld_rem$pau_ducount==0]
a$ind<-0
keep<-data.table()
while (nrow(a)>0){
  for (i in 1:length(unique(a$par_id))){
    print(i)
    if(nrow(a[a$par_id==unique(a$par_id)[i] & a$ind==0])>0){
      keep<-rbind(keep,a[a$par_id==unique(a$par_id)[i] & a$ind==0][sample(1:nrow(a[a$par_id==unique(a$par_id)[i] & a$ind==0]),1,replace=FALSE)])
      a$ind[a$add_locationid==keep$add_locationid[nrow(keep)]]<--1  
    }
  }
  a<-a[!a$par_id%in%keep$par_id]
  a$ind<-0
}
keep<-keep[,!c("addlcomb_id","addlcomb_npar","addlcomb_length","delta_pauducount","addlcomb_pauducount","ind")]
bld_res<-rbind(bld_res,keep)
bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id,]
#pau_ducount>0
b<-bld_rem[bld_rem$par_id%in%temp_rem_exp$par_id]
keep<-data.table()
while (nrow(b)>0){
  for (i in 1:length(unique(b$par_id))){
    print(i)
    if(nrow(b[b$par_id==unique(b$par_id)[i] & b$delta_pauducount>=b$pau_ducount])>0){
      keep<-rbind(keep,b[b$par_id==unique(b$par_id)[i] & b$delta_pauducount>=b$pau_ducount][sample(1:nrow(b[b$par_id==unique(b$par_id)[i] & b$delta_pauducount>=b$pau_ducount]),1,prob=b$delta_pauducount[b$par_id==unique(b$par_id)[i] & b$delta_pauducount>=b$pau_ducount],replace=FALSE)])
      b$delta_pauducount[b$add_locationid==keep$add_locationid[nrow(keep)]]<-b$delta_pauducount[b$add_locationid==keep$add_locationid[nrow(keep)]] - keep$pau_ducount[nrow(keep)]  
    }
  }
  b<-b[!b$par_id%in%keep$par_id]
  b$delta_pauducount<-b$delta_pauducount+1
}
keep<-keep[,!c("addlcomb_id","addlcomb_npar","addlcomb_length","delta_pauducount","addlcomb_pauducount")]
bld_res<-rbind(bld_res,keep)
bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id,!c("addlcomb_id","addlcomb_npar","addlcomb_length","delta_pauducount","addlcomb_pauducount")]

##
#pau_ducount==0
temp<-bld_rem[bld_rem$pau_ducount==0]
temp$ind<-0
keep<-data.table()
while (nrow(temp)>0){
  for (i in 1:length(unique(temp$par_id))){
    print(i)
    if(nrow(temp[temp$par_id==unique(temp$par_id)[i] & temp$ind==0])>0){
      keep<-rbind(keep,temp[temp$par_id==unique(temp$par_id)[i] & temp$ind==0][sample(1:nrow(temp[temp$par_id==unique(temp$par_id)[i] & temp$ind==0]),1,replace=FALSE)])
      temp$ind[temp$add_locationid==keep$add_locationid[nrow(keep)]]<--1  
    }
  }
  temp<-temp[!temp$par_id%in%keep$par_id]
  temp$ind<-0
}
keep<-keep[,!"ind"]
bld_res<-rbind(bld_res,keep)
bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id,]
#pau_ducount>0
temp<-unique(bld[,c("add_locationid","pau_id","pau_ducount")])
agg<-data.table(aggregate(data=temp,pau_ducount~add_locationid,sum))
names(agg)[ncol(agg)]<-"addl_pauducount"
temp_res<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount")])
agg_res<-data.table(aggregate(data=temp_res,pau_ducount~add_locationid,sum))
names(agg_res)[ncol(agg_res)]<-"pau_ducount_res"
agg<-left_join(agg,agg_res)
agg[is.na(agg)]<-0
agg$delta_pauducount<-agg$addl_pauducount-agg$pau_ducount_res
bld_rem<-left_join(bld_rem,agg,by=c("add_locationid"="add_locationid"))
temp<-bld_rem
keep<-data.table()
a<-99999
# while(nrow(keep)!=a){
#   a<-nrow(keep)
#   agg<-data.table(aggregate(data=temp[temp$delta_pauducount==1],par_id~add_locationid,function(x) length(unique(x))))
#   names(agg)[ncol(agg)]<-"addldeltapauducount1_npar"
#   keep<-rbind(keep,temp[temp$add_locationid%in%agg$add_locationid[agg$addldeltapauducount1_npar==1]])
#   temp<-temp[!temp$par_id%in%keep$par_id]
# }
while(nrow(keep)!=a & nrow(temp[temp$delta_pauducount==1])>0){
  a<-nrow(keep)
  agg1<-data.table(aggregate(data=temp[temp$delta_pauducount==1],par_id~add_locationid,function(x) length(unique(x))))
  names(agg1)[ncol(agg1)]<-"addldeltapauducount1_npar"
  agg2<-data.table(aggregate(data=temp[temp$delta_pauducount==1],add_locationid~par_id,function(x) length(unique(x))))
  names(agg2)[ncol(agg2)]<-"par_naddldeltapauducount1"
  # agg<-data.table(aggregate(data=temp[temp$delta_pauducount==1],par_id~add_locationid,function(x) length(unique(x))))
  # names(agg)[ncol(agg)]<-"addldeltapauducount1_npar"
  keep<-rbind(keep,temp[temp$add_locationid%in%agg1$add_locationid[agg1$addldeltapauducount1_npar==1] & temp$par_id%in%agg2$par_id[agg2$par_naddldeltapauducount1==1]])
  temp<-temp[!temp$par_id%in%keep$par_id]
}
while(nrow(temp)>0){
  while(nrow(temp[temp$delta_pauducount>=temp$pau_ducount])>0){
    for (i in 1:length(unique(temp$par_id))){
      print(i)
      if(nrow(temp[temp$par_id==unique(temp$par_id)[i] & temp$delta_pauducount>=temp$pau_ducount])>0){
        keep<-rbind(keep,temp[temp$par_id==unique(temp$par_id)[i] & temp$delta_pauducount>=temp$pau_ducount][sample(1:nrow(temp[temp$par_id==unique(temp$par_id)[i] & temp$delta_pauducount>=temp$pau_ducount]),1,prob=temp$delta_pauducount[temp$par_id==unique(temp$par_id)[i] & temp$delta_pauducount>=temp$pau_ducount],replace=FALSE)])
        temp$delta_pauducount[temp$add_locationid==keep$add_locationid[nrow(keep)]]<-temp$delta_pauducount[temp$add_locationid==keep$add_locationid[nrow(keep)]] - keep$pau_ducount[nrow(keep)]
        temp<-temp[!temp$par_id%in%keep$par_id]
      }
      while(nrow(keep)!=a & nrow(temp[temp$delta_pauducount==1])>0){
        a<-nrow(keep)
        agg1<-data.table(aggregate(data=temp[temp$delta_pauducount==1],par_id~add_locationid,function(x) length(unique(x))))
        names(agg1)[ncol(agg1)]<-"addldeltapauducount1_npar"
        agg2<-data.table(aggregate(data=temp[temp$delta_pauducount==1],add_locationid~par_id,function(x) length(unique(x))))
        names(agg2)[ncol(agg2)]<-"par_naddldeltapauducount1"
        # agg<-data.table(aggregate(data=temp[temp$delta_pauducount==1],par_id~add_locationid,function(x) length(unique(x))))
        # names(agg)[ncol(agg)]<-"addldeltapauducount1_npar"
        keep<-rbind(keep,temp[temp$add_locationid%in%agg1$add_locationid[agg1$addldeltapauducount1_npar==1] & temp$par_id%in%agg2$par_id[agg2$par_naddldeltapauducount1==1]])
        temp<-temp[!temp$par_id%in%keep$par_id]
      }
    }
  }
  temp$delta_pauducount<-temp$delta_pauducount+1
}
keep<-keep[,!c("addl_pauducount","pau_ducount_res","delta_pauducount")]
bld_res<-rbind(bld_res,keep)
bld_rem<-bld_rem[!bld_rem$par_id%in%bld_res$par_id,!c("addl_pauducount","pau_ducount_res","delta_pauducount")]


###bld database
temp_pau<-unique(bld_res[,c("add_locationid","pau_id","pau_ducount","pau_floors","pau_consyr","pau_landarea","pau_bldarea","pau_usagecode","pau_usage","pau_dutype")])
agg_pau_sum<-data.table(aggregate(data=temp_pau,cbind(pau_ducount,pau_landarea,pau_bldarea)~add_locationid,sum))
agg_pau_unique<-data.table(aggregate(data=temp_pau,cbind(pau_consyr,pau_usagecode,pau_usage,pau_dutype)~add_locationid,unique))
agg_pau<-left_join(agg_pau_sum,agg_pau_unique)
temp_par<-unique(bld_res[,c("add_locationid","par_id","par_flats","par_floors","par_consyr","par_landarea","par_bldarea","par_use","par_type")])
temp_par<-left_join(temp_par,par[,c("par_id","par_nonflats","par_bldvalue","par_landvalue","par_totvalue")])
agg_par_sum<-data.table(aggregate(data=temp_par,cbind(par_flats,par_nonflats,par_landarea,par_bldarea,par_bldvalue,par_landvalue,par_totvalue)~add_locationid,sum))
agg_par_unique<-data.table(aggregate(data=temp_par,cbind(par_consyr,par_use,par_type)~add_locationid,unique))
agg_par<-left_join(agg_par_sum,agg_par_unique)
bld<-left_join(agg_par,agg_pau)

##bld_flats
bld$bld_flats<-99999
bld$bld_flats[bld$par_flats!=0]<-bld$par_flats[bld$par_flats!=0]
bld$bld_flats[bld$par_flats==0]<-bld$pau_ducount[bld$par_flats==0]
bld<-bld[,!c("par_flats","pau_ducount")]
# filter to bld_ducount!=0
bld<-bld[bld$bld_flats!=0]

##bld_nonflats
bld$bld_nonflats<-bld$par_nonflats
bld<-bld[,!"par_nonflats"]

##bld_consyr
bld$pau_consyr<-as.character(bld$pau_consyr)
bld$par_consyr<-as.character(bld$par_consyr)
bld$par_consyr[bld$par_consyr=="0" & bld$pau_consyr!="0"]<-bld$pau_consyr[bld$par_consyr=="0" & bld$pau_consyr!="0"]
bld$par_consyr[grepl("c",bld$par_consyr) & !grepl("c",bld$pau_consyr) & bld$pau_consyr!="0"]<-bld$pau_consyr[grepl("c",bld$par_consyr) & !grepl("c",bld$pau_consyr) & bld$pau_consyr!="0"]
bld$par_consyr[grepl(":",bld$par_consyr) & !grepl("c",bld$pau_consyr) & bld$pau_consyr!="0"]<-bld$pau_consyr[grepl(":",bld$par_consyr) & !grepl("c",bld$pau_consyr) & bld$pau_consyr!="0"]
res<-c()
for (i in 1:length(bld$par_consyr[grepl(",",bld$par_consyr)])){
  a<-bld$par_consyr[grepl(",",bld$par_consyr)][i]
  a<-gsub(pattern=('"'),replacement='',x=a)
  a<-gsub(pattern=(' '),replacement='',x=a)
  a<-substr(a,3,nchar(a)-1)
  a<-unlist(strsplit(a,","))
  a<-max(as.numeric(a))
  res<-c(res,a)
}
bld$par_consyr[grepl(",",bld$par_consyr)]<-res
res<-c()
for (i in 1:length(bld$par_consyr[grepl(":",bld$par_consyr)])){
  a<-bld$par_consyr[grepl(":",bld$par_consyr)][i]
  a<-gsub(pattern=('"'),replacement='',x=a)
  a<-gsub(pattern=(' '),replacement='',x=a)
  a<-unlist(strsplit(a,":"))
  a<-max(as.numeric(a))
  res<-c(res,a)
}
bld$par_consyr[grepl(":",bld$par_consyr)]<-res
add_loc_consyrnull<-unique(add[add$add_locationid%in%bld$add_locationid[bld$par_consyr=="0"],c("add_locationid","add_lon","add_lat")])
par_loc_consyrnotnull<-unique(par[!(par$par_locationid%in%par$par_locationid[par$par_consyr==0]),c("par_lon","par_lat","par_locationid")])
# write.csv(add_loc_consyrnull,"Input/Addresses/add_loc_consyrnull.csv")
# write.csv(par_loc_consyrnotnull,"Input/PAR/par_loc_consyrnotnull.csv")
# each add_location without consyr inherits the consyr of the nearest par_location with consyr
add_consyrnull_par_consyrnotnull<-fread("Input/Addresses/add_consyrnull_par_consyrnotnull_near.csv")
add_consyrnull_par_consyrnotnull<-unique(arrange(left_join(add_consyrnull_par_consyrnotnull,par[,c("par_locationid","par_consyr")],by=c("par_locationid"="par_locationid")),par_locationid))
for (i in as.numeric(names(table(add_consyrnull_par_consyrnotnull$add_locationid)[table(add_consyrnull_par_consyrnotnull$add_locationid)>1]))){
  add_consyrnull_par_consyrnotnull[add_consyrnull_par_consyrnotnull$add_locationid==i]<-add_consyrnull_par_consyrnotnull[add_consyrnull_par_consyrnotnull$add_locationid==i & add_consyrnull_par_consyrnotnull$par_consyr==max(add_consyrnull_par_consyrnotnull$par_consyr[add_consyrnull_par_consyrnotnull$add_locationid==i])][1,]
}
add_consyrnull_par_consyrnotnull<-unique(add_consyrnull_par_consyrnotnull)
bld$par_consyr[bld$par_consyr=="0"]<-add_consyrnull_par_consyrnotnull[order(match(add_consyrnull_par_consyrnotnull$add_locationid,bld$add_locationid[bld$par_consyr=="0"]))]$par_consyr
bld$bld_consyr<-bld$par_consyr
bld<-bld[,!c("par_consyr","pau_consyr")]

##bld_value
bld$bld_value<-bld$par_bldvalue
bld<-bld[,!"par_bldvalue"]

##bld_landvalue
bld$bld_landvalue<-bld$par_landvalue
bld<-bld[,!"par_landvalue"]

##bld_totvalue
bld$bld_totvalue<-bld$par_totvalue
bld<-bld[,!"par_totvalue"]

##bld_dutype
bld$pau_dutype<-as.character(bld$pau_dutype)
bld$bld_dutype[bld$pau_dutype=="Regulier"]<-"Not condominium"
bld$bld_dutype[is.na(bld$bld_dutype)]<-"Condominium"
bld<-bld[,!c("par_type","pau_dutype")]

##bld_area
bld$bld_area<-bld$par_bldarea
bld$bld_area[bld$bld_area==0 & bld$pau_bldarea!=0]<-bld$pau_bldarea[bld$bld_area==0 & bld$pau_bldarea!=0]
bld<-bld[,!c("par_bldarea","pau_bldarea")]
#for remaining add_locationid with addl_npaug==1 & paug_naddl==1, use bf_area and bf_elevation to calculate bld_area
temp<-unique(bld_res[,c("add_locationid","pau_geoid")])
agg<-data.table(aggregate(data=bld_res,pau_geoid~add_locationid,function(x) length(unique(x))))
names(agg)[ncol(agg)]<-"addl_npaug"
temp<-left_join(temp,agg)
agg<-data.table(aggregate(data=bld_res,add_locationid~pau_geoid,function(x) length(unique(x))))
names(agg)[ncol(agg)]<-"paug_naddl"
temp<-left_join(temp,agg)
temp<-temp[temp$add_locationid%in%bld$add_locationid[bld$bld_area==0] & temp$addl_npaug==1 & temp$paug_naddl==1]
pau_bldareanull<-unique(pau[pau$pau_geoid%in%temp$pau_geoid,c("pau_geoid","pau_geo")])
# write.csv(pau_bldareanull,"Input/PAU/pau_bldareanull.csv")
# QGIS : intersection of buildings footprints w pau_geoids that have a null bld_area then multiply by (max elevation/5) considering 5m height for each floor
bf_pau_bldareanull<-unique(fread("Input/Buildings_2D/buildings w elevation_pau_bldareanull_int.csv"))
bf_pau_bldareanull$elevation<-as.numeric(gsub(" ","",gsub(",",".",bf_pau_bldareanull$elevation)))
a<-bf_pau_bldareanull$pau_geoid[bf_pau_bldareanull$bf_area<=5]
b<-bf_pau_bldareanull$pau_geoid[bf_pau_bldareanull$bf_area>5]
int<-a[a%in%b]
bf_pau_bldareanull<-bf_pau_bldareanull[!(bf_pau_bldareanull$pau_geoid%in%int & bf_pau_bldareanull$bf_area<=5)]
temp<-data.table(aggregate(data=bf_pau_bldareanull,elevation~pau_geoid,max))
agg<-data.table(aggregate(data=unique(bf_pau_bldareanull[,!"elevation"]),bf_area~pau_geoid,sum))
temp<-left_join(temp,agg)
temp<-left_join(temp,unique(bld_res[,c("pau_geoid","add_locationid")]))
bld<-left_join(bld,temp[,!"pau_geoid"])
bld$bld_area[!is.na(bld$bf_area)]<-(bld$bf_area*bld$elevation/5)[!is.na(bld$bf_area)]
bld<-bld[,!c("elevation","bf_area")]
#for remaining add_locationid with null bld_area : bld_area=bld_landarea*(bldarea_value/bld_landvalue) 
bld$bld_area[bld$bld_area==0]<-(bld$par_landarea*(bld$bld_value/bld$bld_landvalue))[bld$bld_area==0]

##building attributes
bld<-bld[,!c("par_landarea","par_use","pau_landarea","pau_usagecode","pau_usage")]
add$add_lat[add$add_locationid==160079][2]<-add$add_lat[add$add_locationid==160079][1]
bld<-left_join(bld,unique(add[,c("add_locationid","add_lon","add_lat")]))
bld<-data.table(bld_id=bld$add_locationid,
                bld_lon=bld$add_lon,
                bld_lat=bld$add_lat,
                bld_flats=bld$bld_flats,
                bld_nonflats=bld$bld_nonflats,
                # bld_totunits=bld$bld_flats+bld$bld_nonflats,
                bld_area=bld$bld_area,
                # bld_avgarea=bld$bld_area/(bld$bld_flats+bld$bld_nonflats),
                bld_value=bld$bld_value,
                # bld_avgvalue=bld$bld_value/(bld$bld_flats+bld$bld_nonflats),
                bld_landvalue=bld$bld_landvalue,
                # bld_avglandvalue=bld$bld_landvalue/(bld$bld_flats+bld$bld_nonflats),
                # bld_totvalue=bld$bld_totvalue,
                # bld_avgtotvalue=bld$bld_totvalue/(bld$bld_flats+bld$bld_nonflats),
                bld_consyr=bld$bld_consyr,
                bld_dutype=bld$bld_dutype)
#correction
bld<-data.frame(bld)
bld[is.na(bld)]<-0
bld$bld_loc<-paste(bld$bld_lat,bld$bld_lon,sep=";")
agg_bld.unique<-data.table(aggregate(data=bld,cbind(bld_lon,bld_lat,bld_consyr,bld_dutype)~bld_loc,unique))
agg_bld.sum<-data.table(aggregate(data=bld,cbind(bld_flats,bld_nonflats,bld_area,bld_value,bld_landvalue)~bld_loc,sum))
bld<-data.table(bld_id=1:length(unique(bld$bld_loc)),
                bld_lon=agg_bld.unique$bld_lon,
                bld_lat=agg_bld.unique$bld_lat,
                bld_flats=agg_bld.sum$bld_flats,
                bld_nonflats=agg_bld.sum$bld_nonflats,
                # bld_totunits=agg_bld.sum$bld_flats+agg_bld.sum$bld_nonflats,
                bld_area=agg_bld.sum$bld_area,
                # bld_avgarea=agg_bld.sum$bld_area/(agg_bld.sum$bld_flats+agg_bld.sum$bld_nonflats),
                bld_value=agg_bld.sum$bld_value,
                # bld_avgvalue=agg_bld.sum$bld_value/(agg_bld.sum$bld_flats+agg_bld.sum$bld_nonflats),
                bld_landvalue=agg_bld.sum$bld_landvalue,
                # bld_avglandvalue=agg_bld.sum$bld_landvalue/(agg_bld.sum$bld_flats+agg_bld.sum$bld_nonflats),
                # bld_totvalue=agg_bld.sum$bld_totvalue,
                # bld_avgtotvalue=agg_bld.sum$bld_totvalue/(agg_bld.sum$bld_flats+agg_bld.sum$bld_nonflats),
                bld_consyr=agg_bld.unique$bld_consyr,
                bld_dutype=agg_bld.unique$bld_dutype)




bld<-apply(bld,2,as.character)
write.csv(bld,"Input/BLD/bld.csv")

bld
