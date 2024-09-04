rm(list=ls())
gc()

###load packages
library(data.table)
library(dplyr)
library(sf)
library(purrr)
library(Brobdingnag)
library(splitstackshape)

###import
ods<-fread("Output/ods_final.csv")[,-1]
bld<-fread("Output/bld_clustered.csv")[,-1]
bld_locations_mb<-fread("Input/BLD/bld_locations_mb.csv")[,-1]
names(bld_locations_mb)[ncol(bld_locations_mb)]<-"bld_mb"
bld<-left_join(bld,bld_locations_mb[,c("bld_id","bld_mb")],by=c("bld_id"="bld_id"))
WP<-list.files(path="Output/Weighted population",full.names=TRUE) %>% map_dfr(readRDS)
SP<-list.files(path="Output/Synthetic population",full.names=TRUE) %>% map_dfr(readRDS)
sp_shp<-read_sf('Output/SP.shp')
bld_shp<-read_sf('Input/BLD/bld_locations.shp')

###weight_distance
distmat<-left_join(unique(SP[,c("hh_id","hh_mb")]),unique(bld[,c("bld_id","bld_mb")]),by=c("hh_mb"="bld_mb"))[,!"hh_mb"]
distmat<-left_join(distmat,unique(sp_shp[,c("hh_id","geometry")]))
names(distmat)[ncol(distmat)]<-"hh_geom"
distmat<-left_join(distmat,unique(bld_shp[,c("bld_id","geometry")]))
names(distmat)[ncol(distmat)]<-"bld_geom"
distmat$distance<-as.numeric(st_distance(distmat$hh_geom,distmat$bld_geom,by_element=TRUE))
distmat<-distmat[,!c("hh_geom","bld_geom")]
setorderv(distmat, c("hh_id","distance"))
agg<-aggregate(data=distmat,distance~hh_id,max)
names(agg)[ncol(agg)]<-"max_distance"
distmat<-left_join(distmat,agg)
#convert to km
distmat$distance<-distmat$distance/1000
distmat$max_distance<-distmat$max_distance/1000
#set distance from hh to nearest bld to 0
distmat$test<-paste(distmat$hh_id,distmat$bld_id,sep=";")
SP$test<-paste(SP$hh_id,SP$hh_bld,sep=";")
distmat$distance[distmat$test%in%SP$test]<-0
distmat<-distmat[,!"test"]
SP<-SP[,!"test"]
#exp kernel : (exp(distance(R))/(exp(distance(R))-1))*exp(-x)+(1/(1-exp(distance(R))))
a<-as.brob(distmat$max_distance)
b<-as.brob(distmat$distance)
distmat$weight_distance<-as.numeric((exp(a)/(exp(a)-1))*(1/exp(b))+(1/(1-exp(a))))
#normalize weight_distance
agg<-aggregate(data=distmat,weight_distance~hh_id,sum)
names(agg)[ncol(agg)]<-"sum_weight_distance"
distmat<-left_join(distmat,agg)
distmat$weight_distance<-distmat$weight_distance/distmat$sum_weight_distance
distmat<-distmat[,!"sum_weight_distance"]

###probability(hh ∩ bld)
res<-distmat[,c("hh_id","bld_id","weight_distance")]
res<-left_join(res,unique(SP[,c("hh_id","hh_mb","weight")]))
res<-left_join(res,bld[,c("bld_id","bld_cluster","bld_flats")])
temp<-setorder(SP,"hh_id")
temp<-unique(temp[,c("hh_id",names(temp)[grepl("bld_",names(temp))]),with=FALSE])
res<-left_join(res,data.table(hh_id=sort(rep(temp$hh_id,ncol(temp)-1)),bld_cluster=c("T1","T2","T3","T4","T5","T6","T7","T8"),weight_cluster=c(t(temp[,-1]))))
agg<-aggregate(data=res,weight_cluster~hh_id,sum)
names(agg)[2]<-"agg_weight_cluster"
res<-left_join(res,agg)
res$weight_cluster<-res$weight_cluster/res$agg_weight_cluster
res$probability<-res$weight_cluster*res$weight_distance
setorder(res,-probability)

###spatialization
for (i in sort(unique(res$hh_mb))){
  SSP_i<-data.table()
  res_i<-res[res$hh_mb==i]
  while(sum(res_i$weight)>0){
    print(paste("MB_",i,".",sum(unique(res_i[,c("hh_id","weight")])$weight),sep=""))
    temp<-res_i[res_i$probability==max(res_i$probability)][sample(1:nrow(res_i[res_i$probability==max(res_i$probability)]),1)]
    SSP_i<-rbind(SSP_i,data.table(hh_id=temp$hh_id,bld_id=temp$bld_id,n=min(temp$weight,temp$bld_flats)))
    res_i$weight[res_i$hh_id==temp$hh_id]<-res_i$weight[res_i$hh_id==temp$hh_id]-min(temp$weight,temp$bld_flats)
    res_i$bld_flats[res_i$bld_id==temp$bld_id]<-res_i$bld_flats[res_i$bld_id==temp$bld_id]-min(temp$weight,temp$bld_flats)
    res_i<-res_i[res_i$weight>0 & res_i$bld_flats>0]  
  }
  
  saveRDS(SSP_i,paste("Output/Spatialized synthetic population/SSP_",i,sep=""))
}

###spatialized synthetic population
SSP<-list.files(path="Output/Spatialized synthetic population",full.names=TRUE) %>% map_dfr(readRDS)
SSP<-left_join(SSP,bld[,c("bld_id","bld_mb")])
names(SSP)[1]<-"ods_hh_id"
SSP<-expandRows(SSP,"n")
SSP$hh_id<-1:nrow(SSP)
SSP<-left_join(SSP,bld[,c("bld_id","bld_lat","bld_lon")])
names(SSP)[-1:-3]<-c("hh_id","hh_lat","hh_lon")
SSP<-left_join(SSP,SP[,c("hh_id","hh_size","hh_income","hh_ncars","pp_id","pp_agegrp","pp_sex","pp_status","pp_drlicense","pp_transpass","pp_carshpass","pp_bikeshpass")],by=c("ods_hh_id"="hh_id"))
SSP$pp_id<-paste(SSP$hh_id,substr(SSP$pp_id,7,nchar(SSP$pp_id)),sep="")
SSP<-data.table(hh_ods.id=SSP$ods_hh_id,
                hh_id=SSP$hh_id,
                hh_homelat=SSP$hh_lat,
                hh_homelon=SSP$hh_lon,
                hh_bld=SSP$bld_id,
                hh_mb=SSP$bld_mb,
                hh_size=SSP$hh_size,
                hh_income=SSP$hh_income,
                hh_motorization=SSP$hh_ncars,
                pp_id=SSP$pp_id,
                pp_age=SSP$pp_agegrp,
                pp_sex=SSP$pp_sex,
                pp_occupation=SSP$pp_status,
                pp_drlicense=SSP$pp_drlicense,
                pp_transpass=SSP$pp_transpass,
                pp_carshsub=SSP$pp_carshpass,
                pp_bikeshsub=SSP$pp_bikeshpass
                )
saveRDS(SSP[,!c("hh_ods.id")],"Output/Spatialized synthetic population/SSP")
write.csv(SSP[,!c("hh_ods.id")],"Output/Spatialized synthetic population/SSP.csv")
write.csv(SSP[SSP$hh_id%in%c(1,10000),!c("hh_ods.id")],"Output/SSP_example.csv")

###assessment
temp<-unique(ods[,c("hh_id",names(ods)[grepl("bld_",names(ods))]),with=FALSE])
temp<-data.table(hh_ods.id=rep(temp$hh_id,8),
           bld_cluster=rep(c("T1","T2","T3","T4","T5","T6","T7","T8"),each=length(temp$hh_id)),
           cluster_prob=unlist(temp[,-1]))
temp[,rank:=frank(-cluster_prob,ties.method="first"),by=list(hh_ods.id)]
SSP<-left_join(SSP,bld[,c("bld_id","bld_cluster")],by=c("hh_bld"="bld_id"))
SSP<-left_join(SSP,temp[,c("hh_ods.id","bld_cluster","rank")],by=c("hh_ods.id"="hh_ods.id","bld_cluster"="bld_cluster"))
temp<-unique(SSP[,c("hh_id","rank")])

write.csv(round(table(temp$rank)*100/sum(table(temp$rank)),2),"Output/cluster_rank.csv")

