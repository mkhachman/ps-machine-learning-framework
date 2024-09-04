rm(list=ls())
gc()

###load packages
library(data.table)
library(dplyr)
library(factoextra)
library(NbClust)
library(flexclust)
library(cluster)
library(caret)
library(scutr)
library(randomForest)
library(fastcluster)
library(foreach)
library(doParallel)
library(doSNOW)

###functions
#f1_macro_average
f1_macro_average=function(p,y,c){
  y=factor(y)
  p=factor(p)
  levels(p)=levels(y)
  lev=levels(y)
  Mac_F1=NULL
  for(i in 1:length(lev)){
    L=lev[i]
    act=factor(ifelse(y==L, 1, 0))
    pr=factor(ifelse(p==L, 1, 0))
    levels(pr)=levels(act)
    Mac_F1[i]=mlr::measureF1(act, pr, positive=c)
  }
  return(mean(Mac_F1))
}
#f1_micro_average
f1_micro_average=function(p,y,c){
  y=factor(y)
  p=factor(p)
  levels(p)=levels(y)
  lev=levels(y)
  Mac_F1=NULL
  for(i in 1:length(lev)){
    L=lev[i]
    act=factor(ifelse(y==L, 1, 0))
    pr=factor(ifelse(p==L, 1, 0))
    levels(pr)=levels(act)
    Mac_F1[i]=mlr::measureF1(act, pr, positive=c)*length(act[act==1])
  }
  return(sum(Mac_F1)/length(act))
}

###import
ods<-fread("Input/ODS/ods_processed.csv")[,-1]
bld<-fread("Input/BLD/bld_processed.csv")[,-1]
ods_bld<-fread("Input/ODS/ods_bld.csv")[,-1]

###tune clustering model
# #parallel computing
# cores<-detectCores()
# cls<-makeCluster(cores[1]-1)
# registerDoParallel(cls)
# registerDoSNOW(cls)
# #parameters
# sd<-seq(100,1000,100)
# cl<-seq(2,10,1)
# mc<-c("euclidean","manhattan")
# sm<-seq(100,1000,100)
# #progress bar
# iterations<-length(sd)*length(cl)*length(mc)*length(sm)
# pb<-txtProgressBar(max=iterations,style=3)
# progress<-function(n)setTxtProgressBar(pb,n)
# opts<-list(progress=progress)
# #optimal sd, cl, mc, and sm
# tuneClus<-foreach(sd=sd,.combine='rbind')%:%
#   foreach(cl=cl,.combine='rbind')%:%
#   foreach(mc=mc,.combine='rbind')%:%
#   foreach(sm=sm,.combine='rbind',.options.snow=opts)%dopar%{
#     #load packages
#     library(data.table)
#     library(dplyr)
#     library(cluster)
#     #reproducibility
#     set.seed(sd)
#     #temp
#     temp<-unique(bld[,c("bld_id","bld_flats","bld_area.pu","bld_totvalue.pa","da_dumedvalue")])
#     temp<-cbind(temp[,1],scale(temp[,-1],center=TRUE,scale=TRUE))
#     #clus_model
#     clus_model<-clara(temp[,-1],k=cl,metric=mc,samples=sm,pamLike=TRUE)
#     #summary
#     data.table(seed=sd,clusters=cl,metric=mc,samples=sm,asw=clus_model$silinfo$avg.width)
#   }
# #export summary
# write.csv(tuneClus,"Output/tuneClus.csv")
# #stop parallel computing  
# close(pb)
# stopCluster(cls)
#import result
tuneClus<-fread("Output/tuneClus.csv")[,-1]
#mean asw over the seeds for each combination of cl-mc-sm (id)
tuneClus$id<-paste(tuneClus$clusters,tuneClus$metric,tuneClus$samples,sep="_")
agg<-aggregate(data=tuneClus,asw~id,mean)
names(agg)[ncol(agg)]<-"asw.id"
tuneClus<-left_join(tuneClus,agg)
#max asw per cl
temp<-unique(tuneClus[,!"seed"])
agg<-aggregate(data=temp,`asw.id`~clusters,max)
#loss in terms of asw per cluster added
agg$asw_loss.cl<-(max(agg$asw.id)-agg$asw.id)/(agg$clusters-agg$clusters[agg$asw.id==max(agg$asw.id)])
agg[is.na(agg)]<-0
agg<-agg[agg$asw_loss.cl!=0,]
write.csv(agg,"Output/asw_loss_per_additional_cl.csv")
#parameters selection
cl<-max(agg$clusters[agg$asw_loss.cl==min(agg$asw_loss.cl)])
tuneClus<-tuneClus[tuneClus$clusters==cl]
tuneClus<-tuneClus[tuneClus$asw==max(tuneClus$asw)]
tuneClus<-tuneClus[tuneClus$seed==min(tuneClus$seed)]
tuneClus<-tuneClus[tuneClus$samples==min(tuneClus$samples)]
sd<-tuneClus$seed
mc<-tuneClus$metric
sm<-tuneClus$samples

###clustering
#reproducibility
set.seed(sd)
#temp
temp<-unique(bld[,c("bld_id","bld_flats","bld_area.pu","bld_totvalue.pa","da_dumedvalue")])
temp<-cbind(temp[,1],scale(temp[,-1],center=TRUE,scale=TRUE))
#clus_model
clus_model<-clara(temp[,-1],k=cl,metric=mc,samples=sm,pamLike=TRUE)
bld<-left_join(bld,data.table(bld_id=temp$bld_id, temp.bld_cluster=clus_model$cluster))
bld$bld_cluster<-bld$temp.bld_cluster
bld<-bld[,!c("temp.bld_cluster")]
#join to bld and ods_bld
bld$bld_cluster<-as.factor(paste("T",bld$bld_cluster,sep=""))
write.csv(bld,"Output/bld_clustered.csv")
ods_bld<-left_join(ods_bld,bld[,c("bld_id","bld_cluster")])
#clustering results
temp<-unique(ods_bld[,c("bld_id","bld_flats","bld_area.pu","bld_totvalue.pa","da_dumedvalue","bld_cluster")])
clus_res<-aggregate(data=temp,cbind(bld_flats,bld_area.pu,bld_totvalue.pa,da_dumedvalue)~bld_cluster,mean)
clus_res$da_dumedvalue<-round(clus_res$da_dumedvalue/1000,2)
clus_res$bld_totvalue.pa<-round(clus_res$bld_totvalue.pa/1000,2)
clus_res$bld_area.pu<-round(clus_res$bld_area.pu,2)
clus_res$bld_flats<-round(clus_res$bld_flats,2)
write.csv(clus_res,"Output/clus_res.csv")
#clusters percentages
bld.per<-round(table(bld$bld_cluster)*100/sum(table(bld$bld_cluster)),2)
ods_bld.per<-round(table(ods_bld$bld_cluster)*100/sum(table(ods_bld$bld_cluster)),2)
write.csv(rbind(bld.per,ods_bld.per),"Output/cluster_percentages.csv")

###tuneRandomForest
# #parallel computing
# cores<-detectCores()
# cls<-makeCluster(cores[1]-1)
# registerDoParallel(cls)
# registerDoSNOW(cls)
# #parameters
# sd<-seq(100,1000,100)
# mt<-seq(1,10,1)
# ns<-seq(5,50,5)
# nt<-seq(100,1000,100)
# #progress bar
# iterations<-length(sd)*length(mt)*length(ns)*length(nt)
# pb<-txtProgressBar(max=iterations,style=3)
# progress<-function(n)setTxtProgressBar(pb,n)
# opts<-list(progress=progress)
# #optimal sd, mt, ns, and nt
# tuneRandFor<-foreach(sd=sd,.combine='rbind')%:%
#   foreach(mt=mt,.combine='rbind')%:%
#   foreach(ns=ns,.combine='rbind')%:%
#   foreach(nt=nt,.combine='rbind',.options.snow=opts)%dopar%{
#     #load packages
#     library(data.table)
#     library(dplyr)
#     library(caret)
#     library(randomForest)
#     #f1_macro_average
#     f1_macro_average=function(p,y,c){
#       y=factor(y)
#       p=factor(p)
#       levels(p)=levels(y)
#       lev=levels(y)
#       Mac_F1=NULL
#       for(i in 1:length(lev)){
#         L=lev[i]
#         act=factor(ifelse(y==L, 1, 0))
#         pr=factor(ifelse(p==L, 1, 0))
#         levels(pr)=levels(act)
#         Mac_F1[i]=mlr::measureF1(act, pr, positive=c)
#       }
#       return(mean(Mac_F1))
#     }
#     #f1_micro_average
#     f1_micro_average=function(p,y,c){
#       y=factor(y)
#       p=factor(p)
#       levels(p)=levels(y)
#       lev=levels(y)
#       Mac_F1=NULL
#       for(i in 1:length(lev)){
#         L=lev[i]
#         act=factor(ifelse(y==L, 1, 0))
#         pr=factor(ifelse(p==L, 1, 0))
#         levels(pr)=levels(act)
#         Mac_F1[i]=mlr::measureF1(act, pr, positive=c)*length(act[act==1])
#       }
#       return(sum(Mac_F1)/length(act))
#     }
#     #reproducibility
#     set.seed(sd)
#     #db
#     db<-data.table(ods_bld[,c("bld_id","bld_cluster")],ods_bld[,c("hh_size","hh_incmed","hh_meanage","da_hhmedinc","da_ppdens")])
#     db<-cbind(db[,1:2],scale(db[,-1:-2], center=TRUE, scale=TRUE))
#     #db.train
#     training.samples<-db$bld_cluster %>% createDataPartition(p=0.6, list=FALSE)
#     db.train<-db[training.samples,]
#     #db.cv
#     db.rem<-db[-training.samples,]
#     cv.samples<-db.rem$bld_cluster %>% createDataPartition(p=0.5, list=FALSE)
#     db.cv<-db.rem[cv.samples,]
#     #randomForest
#     model<-randomForest(x=db.train[,-1:-2],y=as.factor(db.train$bld_cluster),mtry=mt,nodesize=ns,ntree=nt,oob.prox=TRUE)
#     pred.train<-predict(model,newdata=db.train[,-1:-2])
#     acc.train<-mean(unlist(db.train[,2])==pred.train)
#     f1.train<-f1_macro_average(pred.train,db.train$bld_cluster,"1")
#     pred.cv<-predict(model,newdata=db.cv[,-1:-2])
#     acc.cv<-mean(unlist(db.cv[,2])==pred.cv)
#     f1.cv<-f1_macro_average(pred.cv,db.cv$bld_cluster,"1")
#     #summary
#     data.table(seed=sd,mtry=mt,nodesize=ns,ntree=nt,acc.train=acc.train,acc.cv=acc.cv,f1.train=f1.train,f1.cv=f1.cv)
#   }
# #export summary
# write.csv(tuneRandFor,"Output/tuneRandFor.csv")
# #stop parallel computing
# close(pb)
# stopCluster(cls)
#import result
tuneRandFor<-fread("Output/tuneRandFor.csv")[,-1]
#mean f1.cv over the seeds for each combination of mt-ns-nt (id)
tuneRandFor$id<-paste(tuneRandFor$mtry,tuneRandFor$nodesize,tuneRandFor$ntree,sep="_")
agg<-aggregate(data=tuneRandFor,f1.cv~id,mean)
names(agg)[ncol(agg)]<-"f1.cv.id"
tuneRandFor<-left_join(tuneRandFor,agg,by=c("id"="id"))
#mean acc.cv over the seeds for each combination of mt-ns-nt (id)
agg<-aggregate(data=tuneRandFor,acc.cv~id,mean)
names(agg)[ncol(agg)]<-"acc.cv.id"
tuneRandFor<-left_join(tuneRandFor,agg,by=c("id"="id"))
#filter to highest macro f1.cv.id
tuneRandFor<-tuneRandFor[tuneRandFor$f1.cv.id==max(tuneRandFor$f1.cv.id)]
#filter to highest acc.cv.id
tuneRandFor<-tuneRandFor[tuneRandFor$acc.cv.id==max(tuneRandFor$acc.cv.id)]
#filter to highest f1.cv
tuneRandFor<-tuneRandFor[tuneRandFor$f1.cv==max(tuneRandFor$f1.cv)]
#filter to minimum mtry, nodesize and ntree
tuneRandFor<-tuneRandFor[tuneRandFor$mtry==min(tuneRandFor$mtry)]
tuneRandFor<-tuneRandFor[tuneRandFor$nodesize==min(tuneRandFor$nodesize)]
tuneRandFor<-tuneRandFor[tuneRandFor$ntree==min(tuneRandFor$ntree)]
#parameters selection
sd<-tuneRandFor$seed[1]
mt<-tuneRandFor$mtry[1]
ns<-tuneRandFor$nodesize[1]
nt<-tuneRandFor$ntree[1]

###randomForest
#reproducibility
set.seed(sd)
#db
db<-data.table(ods_bld[,c("bld_id","bld_cluster")],ods_bld[,c("hh_size","hh_incmed","hh_meanage","da_hhmedinc","da_ppdens")])
db<-cbind(db[,1:2],scale(db[,-1:-2], center=TRUE, scale=TRUE))
#db.train
training.samples<-db$bld_cluster %>% createDataPartition(p=0.6, list=FALSE)
db.train<-db[training.samples,]
#db.cv
db.rem<-db[-training.samples,]
cv.samples<-db.rem$bld_cluster %>% createDataPartition(p=0.5, list=FALSE)
db.cv<-db.rem[cv.samples,]
#db.test
db.test<-db.rem[-cv.samples,]
#randomForest
model<-randomForest(x=db.train[,-1:-2],y=as.factor(db.train$bld_cluster),mtry=mt,nodesize=ns,ntree=nt,oob.prox=TRUE)
pred.train<-predict(model,newdata=db.train[,-1:-2])
acc.train<-mean(unlist(db.train[,2])==pred.train)
f1.train<-f1_macro_average(pred.train,db.train$bld_cluster,"1")
pred.cv<-predict(model,newdata=db.cv[,-1:-2])
acc.cv<-mean(unlist(db.cv[,2])==pred.cv)
f1.cv<-f1_macro_average(pred.cv,db.cv$bld_cluster,"1")
pred.test<-predict(model,newdata=db.test[,-1:-2])
acc.test<-mean(unlist(db.test[,2])==pred.test)
f1.test<-f1_macro_average(pred.test,db.test$bld_cluster,"1")
#summary
RandForSummary<-data.table(seed=sd,mtry=mt,nodesize=ns,ntree=nt,acc.train=acc.train,acc.cv=acc.cv,acc.test=acc.test,f1.train=f1.train,f1.cv=f1.cv,f1.test=f1.test)
write.csv(RandForSummary,"Output/RandForSummary.csv")
confmat_train<-table(db.train$bld_cluster,pred.train)
write.csv(confmat_train,"Output/confmat_train.csv")
confmat_cv<-table(db.cv$bld_cluster,pred.cv)
write.csv(confmat_cv,"Output/confmat_cv.csv")
confmat_test<-table(db.test$bld_cluster,pred.test)
write.csv(confmat_test,"Output/confmat_test.csv")
varImp<-data.table(variable=rownames(varImp(model)),varImp(model))
write.csv(varImp,"Output/varImp.csv")
#probabilities
pred<-predict(model,newdata=db[,-1:-2],type="prob")
ods_bld_prob<-cbind(ods_bld,pred)
write.csv(ods_bld_prob,"Output/ods_bld_prob.csv")
