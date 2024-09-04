rm(list=ls())
gc()

###load packages
library(data.table)
library(dplyr)
library(glmnet)
library(purrr)


###import
ods<-fread("Input/ODS/ods18_mtl.csv")
ods<-ods[ods$sdomi100>=100 & ods$sdomi100<200]
bld<-fread("Output/bld_clustered.csv")[,-1]
ods_bld<-fread("Output/ods_bld_prob.csv")[,-1]

###control variables
ods<-left_join(ods,ods_bld[,c("hh_id","bld_id","T1","T2","T3","T4","T5","T6","T7","T8")],by=c("feuillet"="hh_id"))
ods<-unique(data.table(
  #hh id, count, location, and weight
  hh_id=ods$feuillet,hh_count=1,hh_x=ods$xdomi,hh_y=ods$ydomi,hh_bld=ods$bld_id,hh_mb=ods$sdomi100,hh_weight=ods$faclog_s27,
  #hh characteristics
  hh_size=ods$perslogi,hh_income=ods$revenu,
  #hh transportation characteristics
  hh_ncars=ods$autologi,
  #pp id, count and weight
  pp_id=paste(ods$feuillet,ods$rang,sep=""),pp_count=1,pp_weight=ods$facper_s27,
  #pp characteristics
  pp_agegrp=ods$cohorte,pp_sex=ods$sexe,pp_status=ods$p_statut,
  #pp transportation characteristics
  pp_drlicense=ods$permis,pp_transpass=ods$titre,pp_carshpass=ods$abon_ap,pp_bikeshpass=ods$abon_vls,
  #bld types
  bld_T1=ods$T1,bld_T2=ods$T2,bld_T3=ods$T3,bld_T4=ods$T4,bld_T5=ods$T5,bld_T6=ods$T6,bld_T7=ods$T7,bld_T8=ods$T8
))
#recategorize hh_size
ods$hh_size[ods$hh_size>=5]<-5
#hh_income categories 0,9,10 replacement
set.seed(100)
temp<-unique(ods[,c("hh_id","hh_income")])
temp$hh_income[temp$hh_income==0]<-1
temp$hh_income[temp$hh_income%in%c(9,10)]<-sample(temp$hh_income[!temp$hh_income%in%c(9,10)],length(temp$hh_income[temp$hh_income%in%c(9,10)]), replace=TRUE)
names(temp)[ncol(temp)]<-"hh_income.1"
ods<-left_join(ods,temp,by=c("hh_id"="hh_id"))
ods$hh_income<-ods$hh_income.1
ods<-ods[,!"hh_income.1"]
#recategorize hh_ncars
ods$hh_ncars[ods$hh_ncars>=5]<-5
#recategorize pp_status
ods$pp_status[ods$pp_status>=5]<-5
#recategorize pp_drlicense
ods$pp_drlicense[ods$pp_drlicense>1]<-0
#recategorize pp_transpass
ods$pp_transpass[ods$pp_transpass%in%c(1,2,3,4,5,6,8)]<-1
ods$pp_transpass[ods$pp_transpass%in%c(9,10)]<-0
#recategorize pp_carshpass
ods$pp_carshpass[ods$pp_carshpass>1]<-0
#recategorize pp_bikeshpass
ods$pp_bikeshpass[ods$pp_bikeshpass>1]<-0
#cv
cv<-c("hh_count","hh_size","hh_income","hh_ncars","pp_count","pp_agegrp","pp_sex","pp_status","pp_drlicense","pp_transpass","pp_carshpass","pp_bikeshpass")
#export final ods
write.csv(ods,"Output/ods_final.csv")


###population synthesis
for (i in 1:length(unique(ods$hh_mb))){
  print(paste(i,"/",length(unique(ods$hh_mb)),sep=""))
  #ods_i
  ods_i<-ods[ods$hh_mb==sort(unique(ods$hh_mb))[i]]
  #B_i (raw)
  B_i<-c()
  for (j in 1:length(cv)){
    if(grepl("hh_",cv[j])){
      temp<-unique(ods_i[,c("hh_id","hh_weight",cv[j]),with=FALSE])
      agg<-aggregate(data=temp,hh_weight~eval(parse(text=cv[j])),sum)
      names(agg)[1]<-cv[j]
      agg<-left_join(data.table(unique(ods[,names(ods)==cv[j],with=FALSE])),agg)
      agg[is.na(agg)]<-0
      agg<-arrange(agg,agg[,1])
      B_i<-c(B_i,as.vector(unlist(agg[,2])))
      names(B_i)<-c(names(B_i)[names(B_i)!=""],paste("mb",sort(unique(ods$hh_mb))[i],".",cv[j],unique(unlist(agg[,1])),sep=""))
    }else{
      temp<-unique(ods_i[,c("pp_id","pp_weight",cv[j]),with=FALSE])
      agg<-aggregate(data=temp,pp_weight~eval(parse(text=cv[j])),sum)
      names(agg)[1]<-cv[j]
      agg<-left_join(data.table(unique(ods[,names(ods)==cv[j],with=FALSE])),agg)
      agg[is.na(agg)]<-0
      agg<-arrange(agg,agg[,1])
      B_i<-c(B_i,as.vector(unlist(agg[,2])))
      names(B_i)<-c(names(B_i)[names(B_i)!=""],paste("mb",sort(unique(ods$hh_mb))[i],".",cv[j],unique(unlist(agg[,1])),sep=""))
    }
  }
  #B_i (harmonization)
  temp<-data.table(var=names(B_i),freq=B_i)
  temp$freq[grepl("count",temp$var)]<-round(temp$freq[grepl("count",temp$var)],0)
  for (j in 1:length(cv)){
    if(grepl("hh_",cv[j])){
      temp$freq[grepl(cv[j],temp$var)]*temp$freq[grepl("hh_count",temp$var)]/sum(temp$freq[grepl(cv[j],temp$var)])
    }else{
      temp$freq[grepl(cv[j],temp$var)]*temp$freq[grepl("pp_count",temp$var)]/sum(temp$freq[grepl(cv[j],temp$var)])
    }
  }
  #B_i (integerization)
  for (j in cv[!grepl("count",cv)]){
    rmse<-function(x){
      sqrt(sum((x-B_i[grepl(j,names(B_i))])^2)/length(B_i[grepl(j,names(B_i))]))
    }
    res<-c()
    for (k in 1:1000){
      W1_i<-temp$freq[grepl(j,temp$var)]
      W1int_i<-floor(W1_i)
      W1rem_i<-W1_i-W1int_i
      W1def_i<-round(sum(W1rem_i))
      incr<-sample(length(W1_i),size=W1def_i,prob=W1rem_i)
      W1_i[incr]<-floor(W1_i[incr])+1
      W1_i<-floor(W1_i)
      W1_i<-c(W1_i,rmse(W1_i))
      res<-rbind(res,W1_i)
    }
    W1_i<-res[res[,ncol(res)]==min(res[,ncol(res)]),-ncol(res)]
    if(!is.null(nrow(W1_i))){
      W1_i<-W1_i[1,]
    }
    temp$freq[grepl(j,temp$var)]<-W1_i
  }
  B_i<-temp$freq
  names(B_i)<-temp$var
  #A_i
  A_i<-matrix(0,nrow=length(B_i),ncol=length(unique(ods_i$hh_id)))
  rownames(A_i)<-names(B_i)
  colnames(A_i)<-unique(ods_i$hh_id)
  for (j in 1:length(cv)){
    for (k in min(sort(unlist(unique(ods[,cv[j],with=FALSE])))):length(sort(unlist(unique(ods[,cv[j],with=FALSE]))))){
      if(grepl("hh_",cv[j])){
        A_i[rownames(A_i)[grepl(paste(cv[j],k,"$",sep=""),rownames(A_i))],colnames(A_i)%in%ods_i$hh_id[ods_i[,cv[j],with=FALSE]==k]]<-1
      }else{
        temp<-t(eval(parse(text=paste("table(ods_i$hh_id,ods_i$",cv[j],sep="",")"))))
        A_i[rownames(A_i)[grepl(paste(cv[j],k,"$",sep=""),rownames(A_i))],colnames(A_i)%in%ods_i$hh_id[ods_i[,cv[j],with=FALSE]==k]]<-temp[rownames(temp)[grepl(paste("^",k,"$",sep=""),rownames(temp))],colnames(temp)%in%ods_i$hh_id[ods_i[,cv[j],with=FALSE]==k]]
      }
    }
  }
  #optimization_i (glmnet)
  glmnet_i<-glmnet(A_i,B_i,alpha=1,lambda=0,lower.limits=0,intercept=F,thres=1E-10)
  X_i<-coef(glmnet_i)[,1][-1]
  X_i<-X_i[X_i>0]
  A_i<-A_i[,colnames(A_i)%in%names(X_i)]
  #intmeth: probability to increase a weight is proportional to the decimal
  rmse<-function(x){
    sqrt(sum((A_i%*%x-B_i)^2)/nrow(B_i))
  }
  temp<-c()
  for (k in 1:1000){
    W1_i<-X_i
    W1int_i<-floor(W1_i)
    W1rem_i<-W1_i-W1int_i
    W1def_i<-round(sum(W1rem_i))
    incr<-sample(length(W1_i),size=W1def_i,prob=W1rem_i)
    W1_i[incr]<-floor(W1_i[incr])+1
    W1_i<-floor(W1_i)
    W1_i<-c(W1_i,rmse(W1_i))
    temp<-rbind(temp,W1_i)
  }
  W1_i<-temp[temp[,ncol(temp)]==min(temp[,ncol(temp)]),-ncol(temp)]
  if(!is.null(nrow(W1_i))){
    W1_i<-W1_i[1,]
  }
  W1_i<-W1_i[W1_i>0]
  #synthetic population
  X_i<-data.table(hh_id=as.numeric(names(X_i)),weight=X_i)
  W1_i<-data.table(hh_id=as.numeric(names(W1_i)),weight=W1_i)
  saveRDS(left_join(ods_i[ods_i$hh_id%in%X_i$hh_id],X_i),paste("Output/Weighted population/WP_",i,sep=""))
  saveRDS(left_join(ods_i[ods_i$hh_id%in%W1_i$hh_id],W1_i),paste("Output/Synthetic population/SP_",i,sep=""))
}


###assessment
#ods
ods<-fread("Output/ods_final.csv")[,-1]
names(ods)[names(ods)=="hh_mb"]<-"mb"
ods_hh<-unique(ods[,c("mb",names(ods)[grepl("hh_",names(ods))]),with=FALSE])
ods_hh<-ods_hh[,!c("hh_x","hh_y","hh_bld")]
names(ods_hh)[names(ods_hh)=="hh_weight"]<-"weight"
ods_hh<-arrange(ods_hh,mb,hh_id)
ods_pp<-unique(ods[,c("mb",names(ods)[grepl("pp_",names(ods))]),with=FALSE])
names(ods_pp)[names(ods_pp)=="pp_weight"]<-"weight"
ods_pp<-arrange(ods_pp,mb,pp_id)
#sp
SP<-list.files(path="Output/Synthetic population",full.names=TRUE) %>% map_dfr(readRDS)
SP<-SP[,!c("hh_x","hh_y","hh_bld","hh_weight","pp_weight",names(SP)[grepl("bld_",names(SP))]),with=FALSE]
names(SP)[names(SP)=="hh_mb"]<-"mb"
SP_hh<-unique(SP[,c(names(SP)[grepl("hh_",names(SP))],"mb","weight"),with=FALSE])
SP_hh<-arrange(SP_hh,mb,hh_id)
SP_pp<-unique(SP[,c(names(SP)[grepl("pp_",names(SP))],"mb","weight"),with=FALSE])
SP_pp<-arrange(SP_pp,mb,pp_id)
#cv
cv_hh<-names(ods_hh)[!names(ods_hh)%in%c("hh_id","mb","weight")]
cv_pp<-names(ods_pp)[!names(ods_pp)%in%c("pp_id","mb","weight")]

res<-data.table()
for (i in 1:length(cv_hh)){
  print(i)
  eval(parse(text=paste("temp<-aggregate(data=unique(ods_hh[,names(ods_hh)%in%c(\"mb\",\"hh_id\",\"weight\",cv_hh[i]),with=FALSE]),weight~",cv_hh[i],"+mb,sum)")))
  names(temp)<-c("var","mb","weight")
  temp_ods<-left_join(data.table(mb=sort(rep(unique(ods_hh$mb),length(unique(unlist(ods_hh[,names(ods_hh)==cv_hh[i],with=FALSE]))))),var=rep(sort(unlist(unique(ods_hh[,names(ods_hh)==cv_hh[i],with=FALSE]))),length(unique(ods_hh$mb)))),temp)
  names(temp_ods)[ncol(temp_ods)]<-"target"
  eval(parse(text=paste("temp<-aggregate(data=unique(SP_hh[,names(SP_hh)%in%c(\"mb\",\"hh_id\",\"weight\",cv_hh[i]),with=FALSE]),weight~",cv_hh[i],"+mb,sum)")))
  names(temp)<-c("var","mb","weight")
  temp_ods_sp<-left_join(temp_ods,temp)
  temp_ods_sp$var<-paste(cv_hh[i],temp_ods_sp$var,sep=".")
  res<-rbind(res,temp_ods_sp)
}
for (i in 1:length(cv_pp)){
  print(i)
  eval(parse(text=paste("temp<-aggregate(data=unique(ods_pp[,names(ods_pp)%in%c(\"mb\",\"pp_id\",\"weight\",cv_pp[i]),with=FALSE]),weight~",cv_pp[i],"+mb,sum)")))
  names(temp)<-c("var","mb","weight")
  temp_ods<-left_join(data.table(mb=sort(rep(unique(ods_pp$mb),length(unique(unlist(ods_pp[,names(ods_pp)==cv_pp[i],with=FALSE]))))),var=rep(sort(unlist(unique(ods_pp[,names(ods_pp)==cv_pp[i],with=FALSE]))),length(unique(ods_pp$mb)))),temp)
  names(temp_ods)[ncol(temp_ods)]<-"target"
  eval(parse(text=paste("temp<-aggregate(data=unique(SP_pp[,names(SP_pp)%in%c(\"mb\",\"pp_id\",\"weight\",cv_pp[i]),with=FALSE]),weight~",cv_pp[i],"+mb,sum)")))
  names(temp)<-c("var","mb","weight")
  temp_ods_sp<-left_join(temp_ods,temp)
  temp_ods_sp$var<-paste(cv_pp[i],temp_ods_sp$var,sep=".")
  res<-rbind(res,temp_ods_sp)
}
res[is.na(res)]<-0
write.csv(res,"Output/SynPopRes_MB.csv")
