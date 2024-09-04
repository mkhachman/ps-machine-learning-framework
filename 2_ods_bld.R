rm(list=ls())
gc()
library(data.table)
library(dplyr)

###import ods
ods<-fread("Input/ODS/ods18_mtl.csv")

###rename variables
ods<-data.table(
  #unique id
  uid=ods$ipere, 
  #hh id and weight
  hh_id=ods$feuillet, hh_weight=ods$faclog_s27,
  #hh location
  hh_x=ods$xdomi, hh_y=ods$ydomi, hh_zipcode=ods$cdomi, hh_reg=ods$regdomi, hh_mb=ods$sdomi100, hh_csd=ods$sdrdomi, hh_ct=ods$srdomi,
  #hh characteristics
  hh_size=ods$perslogi, hh_income=ods$revenu, hh_type=ods$TypeMenage, hh_lang=ods$langue,
  #hh transportation characteristics
  hh_ncars=ods$autologi,
  #pp id
  pp_id=paste(ods$feuillet,ods$rang,sep=""), 
  #pp characteristics
  pp_age=ods$age, pp_agegrp=ods$cohorte, pp_sex=ods$sexe, pp_lfst=ods$p_statut, pp_inab=ods$incap,
  #pp work characteristics
  pp_remote=ods$tele_trav, pp_uswrkpl=ods$lieu_habit,
  ##pp transportation characteristics
  pp_drlicense=ods$permis, pp_carshpass=ods$abon_ap, pp_transpass=ods$titre, pp_bikeshpass=ods$abon_vls, jr_seqmodes=ods$seq_modes
)

###Fabre's clustering variables
#hh_meanage
agg<-aggregate(data=unique(ods[,c("hh_id","hh_size","pp_id","pp_age")]),pp_age~hh_id,mean)
names(agg)[ncol(agg)]<-"hh_meanage"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
#hh_maxage
agg<-aggregate(data=unique(ods[,c("hh_id","hh_size","pp_id","pp_age")]),pp_age~hh_id,max)
names(agg)[ncol(agg)]<-"hh_maxage"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
#hh_minage
agg<-aggregate(data=unique(ods[,c("hh_id","hh_size","pp_id","pp_age")]),pp_age~hh_id,min)
names(agg)[ncol(agg)]<-"hh_minage"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
#hh_ampage
ods$hh_ampage<-ods$hh_maxage-ods$hh_minage
#hh_children
agg<-aggregate(data=unique(ods[ods$pp_age<12,c("hh_id","hh_size","pp_id","pp_age")]),pp_age~hh_id,length)
names(agg)[ncol(agg)]<-"hh_children"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
ods$hh_children[is.na(ods$hh_children)]<-0
#hh_elderly
agg<-aggregate(data=unique(ods[ods$pp_age>=76 & ods$pp_age<=84,c("hh_id","hh_size","pp_id","pp_age")]),pp_age~hh_id,length)
names(agg)[ncol(agg)]<-"hh_elderly"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
ods$hh_elderly[is.na(ods$hh_elderly)]<-0

###first mode used
#jr_fm (1-auto, 2-transit, 3-bikesh, 4-other)
ods$jr_fm<-sub("\\s.*","",ods$jr_seqmodes)
ods$temp[ods$jr_fm==""]<-"0"
ods$temp[nchar(ods$jr_fm)%in%c(1,2)]<-ods$jr_fm[nchar(ods$jr_fm)%in%c(1,2)]
ods$temp[nchar(ods$jr_fm)>2 & as.numeric(substr(ods$jr_fm,1,1))==1]<-substr(ods$jr_fm[nchar(ods$jr_fm)>2 & as.numeric(substr(ods$jr_fm,1,1))==1],1,2)
ods$temp[nchar(ods$jr_fm)>2 & as.numeric(substr(ods$jr_fm,1,1))>1]<-substr(ods$jr_fm[nchar(ods$jr_fm)>2 & as.numeric(substr(ods$jr_fm,1,1))>1],1,1)
ods$temp[as.numeric(ods$temp)>18]<-substr(ods$temp[as.numeric(ods$temp)>18],1,1)
ods$jr_fm<-as.numeric(ods$temp)
ods<-ods[,!"temp"]
ods$temp[ods$jr_fm%in%c(1,2)]<-1
ods$temp[ods$jr_fm%in%c(3,4,5,6,7,8)]<-2
ods$temp[ods$jr_fm==13 & ods$pp_bikeshpass==1]<-3
ods$temp[ods$jr_fm==13 & ods$pp_bikeshpass!=1]<-4
ods$temp[!ods$jr_fm%in%c(1,2,3,4,5,6,7,8,13)]<-4
ods$jr_fm<-ods$temp
ods<-ods[,!"temp"]
#convert jr_fm to binary variables
ods$jr_fm.auto[ods$jr_fm==1]<-1
ods$jr_fm.auto[is.na(ods$jr_fm.auto)]<-0
ods$jr_fm.transit[ods$jr_fm==2]<-1
ods$jr_fm.transit[is.na(ods$jr_fm.transit)]<-0
ods$jr_fm.bikesh[ods$jr_fm==3]<-1
ods$jr_fm.bikesh[is.na(ods$jr_fm.bikesh)]<-0
ods$jr_fm.other[ods$jr_fm==4]<-1
ods$jr_fm.other[is.na(ods$jr_fm.other)]<-0
#hh_fm & %hh_fm
agg<-aggregate(data=ods,jr_fm.auto~hh_id,sum)
names(agg)[ncol(agg)]<-gsub("jr","hh",names(agg)[ncol(agg)])
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
agg<-aggregate(data=ods,jr_fm.transit~hh_id,sum)
names(agg)[ncol(agg)]<-gsub("jr","hh",names(agg)[ncol(agg)])
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
agg<-aggregate(data=ods,jr_fm.bikesh~hh_id,sum)
names(agg)[ncol(agg)]<-gsub("jr","hh",names(agg)[ncol(agg)])
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
agg<-aggregate(data=ods,jr_fm.other~hh_id,sum)
names(agg)[ncol(agg)]<-gsub("jr","hh",names(agg)[ncol(agg)])
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
agg<-aggregate(data=ods,uid~hh_id,length)
names(agg)[ncol(agg)]<-"hh_jrcount"
ods<-left_join(ods,agg,by=c("hh_id"="hh_id"))
temp<-(ods[,grepl("hh_fm",names(ods)),with=FALSE]/ods$hh_jrcount)
names(temp)<-paste("%",names(temp),sep="")
ods<-cbind(ods,temp)

###mobility passes' variables
#hh_drlicense
temp<-unique(ods[,!names(ods)%in%c("uid",names(ods)[grepl("jr_",names(ods))]),with=FALSE])
temp$pp_drlicense[temp$pp_drlicense!=1]<-0
agg<-data.table(aggregate(data=temp,pp_drlicense~hh_id,sum))
names(agg)[ncol(agg)]<-"hh_drlicense"
temp<-left_join(temp,agg,by=c("hh_id"="hh_id"))
ods<-left_join(ods,unique(temp[,c("hh_id","hh_drlicense")]),by=c("hh_id"="hh_id"))
#hh_transpass
temp<-unique(ods[,!names(ods)%in%c("uid",names(ods)[grepl("jr_",names(ods))]),with=FALSE])
temp$pp_transpass[temp$pp_transpass<8]<-1
temp$pp_transpass[temp$pp_transpass>=8]<-0
agg<-data.table(aggregate(data=temp,pp_transpass~hh_id,sum))
names(agg)[ncol(agg)]<-"hh_transpass"
temp<-left_join(temp,agg,by=c("hh_id"="hh_id"))
ods<-left_join(ods,unique(temp[,c("hh_id","hh_transpass")]),by=c("hh_id"="hh_id"))
#hh_bikeshpass
temp<-unique(ods[,!names(ods)%in%c("uid",names(ods)[grepl("jr_",names(ods))]),with=FALSE])
temp$pp_bikeshpass[temp$pp_bikeshpass!=1]<-0
agg<-data.table(aggregate(data=temp,pp_bikeshpass~hh_id,sum))
names(agg)[ncol(agg)]<-"hh_bikeshpass"
temp<-left_join(temp,agg,by=c("hh_id"="hh_id"))
ods<-left_join(ods,unique(temp[,c("hh_id","hh_bikeshpass")]),by=c("hh_id"="hh_id"))

###hh_location
ods$hh_location<-paste(ods$hh_x,ods$hh_y,sep=";")

###hh_income variable processing
#hh_income categories 0,9,10 replacement
set.seed(100)
temp<-unique(ods[,c("hh_id","hh_income")])
temp$hh_income[temp$hh_income==0]<-1
temp$hh_income[temp$hh_income%in%c(9,10)]<-sample(temp$hh_income[!temp$hh_income%in%c(9,10)],length(temp$hh_income[temp$hh_income%in%c(9,10)]), replace=TRUE)
names(temp)[ncol(temp)]<-"hh_income.1"
ods<-left_join(ods,temp,by=c("hh_id"="hh_id"))
ods$hh_income<-ods$hh_income.1
ods<-ods[,!"hh_income.1"]
#replace hh_income with median values of corresponding classes
ods$hh_incmed[ods$hh_income==1]<-15000
ods$hh_incmed[ods$hh_income==2]<-45000
ods$hh_incmed[ods$hh_income==3]<-75000
ods$hh_incmed[ods$hh_income==4]<-105000
ods$hh_incmed[ods$hh_income==5]<-135000
ods$hh_incmed[ods$hh_income==6]<-165000
ods$hh_incmed[ods$hh_income==7]<-195000
ods$hh_incmed[ods$hh_income==8]<-225000

###filter to Montreal island and keep only household variables
ods<-unique(ods[ods$hh_mb>=100 & ods$hh_mb<200,grepl("hh_",names(ods)),with=FALSE])
write.csv(ods,"Input/ODS/ods_processed.csv")


#################################################################################################################


###import bld
bld<-fread("Input/BLD/bld.csv")[,-1]

###convert bld_dutype to numeric
bld$bld_dutype[bld$bld_dutype=="Condominium"]<-1
bld$bld_dutype[bld$bld_dutype=="Not condominium"]<-0
bld$bld_dutype<-as.numeric(bld$bld_dutype)

###derived variables
bld$bld_area.pu<-bld$bld_area/(bld$bld_flats+bld$bld_nonflats)
bld$bld_value.pu<-bld$bld_value/(bld$bld_flats+bld$bld_nonflats)
bld$bld_value.pa<-bld$bld_value/bld$bld_area
bld$bld_landvalue.pu<-bld$bld_landvalue/(bld$bld_flats+bld$bld_nonflats)
bld$bld_landvalue.pa<-bld$bld_landvalue/bld$bld_area
bld$bld_totvalue<-bld$bld_value+bld$bld_landvalue
bld$bld_totvalue.pu<-bld$bld_totvalue/(bld$bld_flats+bld$bld_nonflats)
bld$bld_totvalue.pa<-bld$bld_totvalue/bld$bld_area

###da characteristics
#Population density per square kilometre                                                     6
#Average household size                                                                     58
#Average size of census families                                                            73
#Total - Private households by household type - 100% data                                   92:99
#Median after-tax income of households in 2015 ($)                                          743
#Average after-tax income of households in 2015 ($)                                         752
#Total - Household after-tax income groups in 2015 for private households - 100% data       780:799
#Total - Occupied private dwellings by number of bedrooms - 25% sample data                 1624:1629
#Total - Occupied private dwellings by number of rooms - 25% sample data                    1630:1635
#Average number of rooms per dwelling                                                       1636
#Total - Private households by number of household maintainers - 25% sample data            1654:1657
#Median monthly shelter costs for owned dwellings ($)                                       1674
#Average monthly shelter costs for owned dwellings ($)                                      1675
#Median value of dwellings ($)                                                              1676
#Average value of dwellings ($)                                                             1677
#Median monthly shelter costs for rented dwellings ($)                                      1681
#Average monthly shelter costs for rented dwellings ($)                                     1682
bld_locations<-unique(bld[,c("bld_id","bld_lon","bld_lat")])
write.csv(bld_locations,"Input/BLD/bld_locations.csv")
#QGIS: intersect bld_locations with DAs ==> bld_locations_da
bld_locations_da<-fread("Input/BLD/bld_locations_da.csv")[,c("bld_id","DAUID")]
names(bld_locations_da)<-c("bld_id","da_id")
bld<-left_join(bld, bld_locations_da)
DAT<-fread("Input/Census/DA/MTL_98-401-X2016044_English_CSV_data.csv")
DAT<-data.table(da_id=DAT$`GEO_CODE (POR)`, da_var=DAT$`DIM: Profile of Dissemination Areas (2247)`, da_varid= DAT$`Notes: Profile of Dissemination Areas (2247)`, da_catid=DAT$`Member ID: Profile of Dissemination Areas (2247)`, da_total=DAT$`Dim: Sex (3): Member ID: [1]: Total - Sex`, da_men=DAT$`Dim: Sex (3): Member ID: [2]: Male`, da_women=DAT$`Dim: Sex (3): Member ID: [3]: Female`)
DAT$da_total<-as.numeric(DAT$da_total)
DAT$da_men<-as.numeric(DAT$da_men)
DAT$da_women<-as.numeric(DAT$da_women)
DAT[is.na(DAT)]<-0
DAT<-DAT[!DAT$da_id%in%DAT$da_id[(DAT$da_catid==1 & DAT$da_total==0)|(DAT$da_catid==5 & DAT$da_total==0)]] #only keep DAs with a population and private dwellings occupied by usual residents
setorder(DAT,da_id)
bld<-left_join(bld,DAT[DAT$da_catid%in%6,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_ppdens"
bld<-left_join(bld,DAT[DAT$da_catid%in%58,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_hhavgsize"
bld<-left_join(bld,DAT[DAT$da_catid%in%743,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_hhmedinc"
bld<-left_join(bld,DAT[DAT$da_catid%in%752,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_hhavginc"
bld<-left_join(bld,DAT[DAT$da_catid%in%1636,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_duavgroom"
bld<-left_join(bld,DAT[DAT$da_catid%in%1676,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_dumedvalue"
bld<-left_join(bld,DAT[DAT$da_catid%in%1677,c("da_id","da_total")])
names(bld)[ncol(bld)]<-"da_duavgvalue"

###mobility variables
#transit stops
stop_bld.exo_bus<-fread("Input/GTFS/2018/stop_bld.exo_bus.csv")
agg.1<-aggregate(data=stop_bld.exo_bus,stop_name~bld_id,function(x) length(unique(x)))
stop_bld.exo_train<-fread("Input/GTFS/2018/stop_bld.exo_train.csv")
agg.2<-aggregate(data=stop_bld.exo_train,stop_name~bld_id,function(x) length(unique(x)))
stop_bld.stm<-fread("Input/GTFS/2018/stop_bld.stm.csv")
agg.3<-aggregate(data=stop_bld.stm,stop_name~bld_id,function(x) length(unique(x)))
stop_bld.rtl<-fread("Input/GTFS/2018/stop_bld.rtl.csv")
agg.4<-aggregate(data=stop_bld.rtl,stop_name~bld_id,function(x) length(unique(x)))
stop_bld.stl<-fread("Input/GTFS/2018/stop_bld.stl.csv")
agg.5<-aggregate(data=stop_bld.stl,stop_name~bld_id,function(x) length(unique(x)))
stop_bld.bixi<-fread("Input/GBFS/2018/stop_bld.bixi.csv")
agg<-rbind(agg.1,agg.2,agg.3,agg.4,agg.5)
agg<-aggregate(data=agg,stop_name~bld_id,function(x) length(unique(x)))
names(agg)[ncol(agg)]<-"bld_ts"
bld<-left_join(bld,agg)
bld$bld_ts[is.na(bld$bld_ts)]<-0
#bikesh stations
agg<-aggregate(data=stop_bld.bixi,name~bld_id,function(x) length(unique(x)))
names(agg)[ncol(agg)]<-"bld_bss"
bld<-left_join(bld,agg)
bld$bld_bss[is.na(bld$bld_bss)]<-0

###corrections
#remove bld_area==0
bld<-bld[bld$bld_area!=0]
#remove da variables' NAs
bld<-bld[!is.na(bld$da_hhavgsize)]
#da_dumedvalue imputation
agg<-aggregate(data=bld,bld_totvalue.pu~bld_id,median)
names(agg)<-c("bld_id","temp.da_dumedvalue")
bld<-left_join(bld,agg)
bld$da_dumedvalue[bld$da_dumedvalue==0]<-bld$temp.da_dumedvalue[bld$da_dumedvalue==0]
bld<-bld[,!"temp.da_dumedvalue"]
#da_duavgvalue imputation
agg<-aggregate(data=bld,bld_totvalue.pu~bld_id,mean)
names(agg)<-c("bld_id","temp.da_duavgvalue")
bld<-left_join(bld,agg)
bld$da_duavgvalue[bld$da_duavgvalue==0]<-bld$temp.da_duavgvalue[bld$da_duavgvalue==0]
bld<-bld[,!"temp.da_duavgvalue"]

###export
write.csv(bld,"Input/BLD/bld_processed.csv")


#################################################################################################################


###ods_bld
#ods_locations
ods_locations<-unique(ods[,c("hh_location","hh_x","hh_y")])
write.csv(ods_locations,"Input/ODS/ods_locations.csv")
#bld_locations
bld_locations<-unique(bld[,c("bld_id","bld_lon","bld_lat")])
write.csv(bld_locations,"Input/BLD/bld_locations.csv")
#QGIS : join ods_locations to nearest bld_locations ==> ods_bld_locations
ods_bld_locations<-fread("Input/ODS/ods_bld_locations.csv")
names(ods_bld_locations)[names(ods_bld_locations)%in%"hh_locatio"]<-"hh_location"
#join ods and bld
ods_bld<-left_join(ods_bld_locations[,c("hh_location","bld_id")],ods[,c("hh_location","hh_id")],by=c("hh_location"="hh_location"))[,!"hh_location"]
ods_bld<-left_join(ods_bld,ods)
ods_bld<-left_join(ods_bld,bld)
ods_bld<-cbind(ods_bld[,grepl("hh_",names(ods_bld)),with=FALSE],ods_bld[,grepl("bld_",names(ods_bld)),,with=FALSE],ods_bld[,grepl("da_",names(ods_bld)),,with=FALSE])
write.csv(ods_bld,"Input/ODS/ods_bld.csv")
