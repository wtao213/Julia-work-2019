########################################################################
##      start on Oct 24th,2019
##   logistic regression for predict  potential rsp ##
##
###########################################################################

### pre setting for the code
ENV["COLUMNS"]=240
ENV["LINES"] = 60

using CSV
using FreqTables
using StatsBase
using Plots
using ODBC
using DataFrames
using JLD2
using Dates
using Tables
using CategoricalArrays
using GLM
using StatsModels
using Statistics


#############################################################################

## read the cusotmer file
df = CSV.read("C:\\Users\\012790\\Desktop\\RSP\\rsp_base_equity139k.csv",missingstrings =["NULL",""])
pro= CSV.read("C:\\Users\\012790\\Desktop\\RSP\\rsp_profit_139k.csv",missingstrings =["NULL",""])

## merge the two dataframe
df_full= join(df,pro,on=:PrimaryClientID ,kind= :inner)

##

[names(df_full) eltypes(df_full)]

freqtable(df_full[!,:rsp_act])

df_full[!,:target] = [ x==0 ? 0 : 1 for x in df_full[!,:rsp_act]]


freqtable(df_full[!,:classtype],df_full[!,:target])

CSV.write("C:\\Users\\012790\\Desktop\\RSP\\rsp_cus_full.csv",df_full)











##############################################################################
## get your data, analysis start from here
df = CSV.read("C:\\Users\\012790\\Desktop\\RSP\\rsp_cus_full.csv")

## we don't want to look at who start with rsp, only look at  who rist  is not rsp
df2= df[[x in ["SD","WM","multi-class"] for x in df[!,:classtype]],:]

df2= df2[completecases(df2,[:age_dec18,:rev_ly, :exp_ly, :profit_ly]),:]
[names(df2) eltypes(df2)]
df2[!,:EquityCAD_dec18] = [ismissing(x) ? 0 : x for x in df2[!,:EquityCAD_dec18]]
df2[!,:EquityCAD_may19] = [ismissing(x) ? 0 : x for x in df2[!,:EquityCAD_may19]]
df2[!,:EquityCAD_diff] = df2[!,:EquityCAD_may19] - df2[!,:EquityCAD_dec18]

#####
## a = df2[:, [:,:age_dec18,:rev_ly, :exp_ly, :profit_ly]]
## a = a[completecases(a), :]


freqtable(df2[!,:classtype],df2[!,:target])
freqtable(df2[!,:province],df2[!,:target])


freqtable(df2[!,:age_dec18],df2[!,:target])

## quickly run logit plot to test
## using median stead of the mean of x variable, but need using Statistics

function logit_plot(df::DataFrame,x::Symbol,k::Integer,y::Symbol)

      df[!,:rank] =  ceil.(Int,tiedrank(df[!,x])*k/(length(df[!,x]) +1))
      df1 = by(df,:rank, target = y =>mean, a = x =>median, n= y=>length, cls = y => sum)
      df1[!,:logit] = [log( (c + 1)/ (d - c + 1) ) for (c,d) in zip(df1[!,:cls],df1[!,:n])]

      scatter(df1[!,:a],df1[!,:logit],xlabel=x,ylabel="logit target",linealpha=0.1,legend = false)

end

[names(df2) eltypes(df2)]
logit_plot(df2,:join_month,20,:target)
logit_plot(df2,:age_dec18,20,:target)
logit_plot(df2,:EquityCAD_dec18,20,:target)
logit_plot(df2,:EquityCAD_may19,20,:target)
logit_plot(df2,:EquityCAD_diff,20,:target)


logit_plot(df2,:MTdec18,20,:target)
logit_plot(df2,:rev_ly ,20,:target)
logit_plot(df2,:exp_ly ,20,:target)
logit_plot(df2,:profit_ly ,20,:target)

summarystats(df2[!,:exp_ly])
size(df,1)

## profit of customer during sep to nov
logit_plot(df2,:rev_09_11 ,20,:target)
logit_plot(df2,:exp_09_11 ,20,:target)
logit_plot(df2,:profit_09_11 ,20,:target)


## profit of customer during  nov
logit_plot(df2,:rev_11 ,20,:target)
logit_plot(df2,:exp_11 ,20,:target)
logit_plot(df2,:profit_11 ,20,:target)


median([1,2,3])

### issue with logit plot
### 1. want stable y range
### 2. need to consider missing value


## distribution plot of equity at dec 18
histogram(df2[!,:EquityCAD_dec18],group=df2[!,:target],fillalpha=0.4,linealpha=0.1,nbins=0:2500:70000,title="equity",xlabel="Equity Dec 2018",ylabel="customer ct")


## add in attributes about profitbility,customer trade info, equity info





#################################################################################
## write your model
##
##
[names(df2) eltypes(df2)]
lm =glm(@formula(target ~ age_dec18 + MTdec18 + EquityCAD_dec18 + exp_ly ),df2,Binomial(),LogitLink())

















#################################################################################################################
## part 4: overall checking
## from 1. net new customer first account, 2. open from exisiting customer
##      3. for who already have, what's the increased contribution
## look at overall customer who has rsp ( but remove client 2767, which have more than 200 accounts....)

## read the file
rsp  = CSV.read("C:\\Users\\012790\\Desktop\\RSP\\RSP_act_overall115k.csv",missingstrings =["NULL",""])

[names(rsp) eltypes(rsp)]

## get the difference of the equity
rsp[!,:EquityCAD_dec18] = [ismissing(x) ? 0 : x for x in rsp[!,:EquityCAD_dec18]]
rsp[!,:EquityCAD_may19] = [ismissing(x) ? 0 : x for x in rsp[!,:EquityCAD_may19]]
rsp[!,:equity_diff] = rsp[!,:EquityCAD_may19] - rsp[!,:EquityCAD_dec18]

rsp[!,:TotalAssets_ttl_rsp] = [ismissing(x) ? 0 : x for x in rsp[!,:TotalAssets_ttl_rsp]]

t1 = freqtable(rsp[!,:rank_join])
freqtable(rsp[!,:rank_ind])
freqtable(rsp[!,:create_time_ind])
prop(t1)

t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status])
prop(t1,1)

## to only get a subset freqtable!!!!
t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status],subset=rsp[!,:rank_join] .< 5)
t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status],subset=[x in ["Complete","Closed"] for x in rsp[!,:STA_Status]])
prop(t1,1)
prop(t1,2)

## create ind for the act is first act of customer or not
t1= freqtable(rsp[!,:act_create_yr],rsp[!,:rank_ind])
prop(t1,1)
prop(t1,2)


## want to know the increamental of the customer already opened euiqty
## waht to look at the accout is not close before
freqtable(rsp[!,:create_time_ind])
freqtable(rsp[!,:close_ind])

df = rsp[(rsp.close_ind .!= "close before dec 18") .& (rsp.create_time_ind .!= " join after") ,:]
## aggregate these accounts to customer level
[names(df) eltypes(df)]

## look at account equity change
##nbins=0:1:102,
quantile(df[!,:equity_diff],(0.02,0.98))
histogram(df[!,:equity_diff],group=df[!,:create_time_ind],fillalpha=0.4,linealpha=0.1, nbins = -20000:5000:80000,
            title="Equity CAD change during Dec 18 to May 19",xlabel="CAD",ylabel="account Count")


## look at customer external funding change
quantile(df[!,:equity_diff],(0.02,0.98))
histogram(df[!,:equity_diff],group=df[!,:create_time_ind],fillalpha=0.4,linealpha=0.1, nbins = -20000:5000:80000,
            title="Equity CAD change during Dec 18 to May 19",xlabel="CAD",ylabel="account Count")

by(df,:create_time_ind,N = :equity_diff =>length, avg= :equity_diff =>mean)

t1 = by(df,:create_time_ind, [:equity_diff] =>
      x-> (N = length(x.equity_diff), avg= mean(x.equity_diff),q01=quantile(x.equity_diff,0.01)
            ,q02=quantile(x.equity_diff,0.02),q1=quantile(x.equity_diff,0.25),median=quantile(x.equity_diff,0.5)
            ,q3=quantile(x.equity_diff,0.75),q98=quantile(x.equity_diff,0.98)))

t1 = by(df,:create_time_ind, [:TotalAssets_ttl_rsp] =>
      x-> (N = length(x.TotalAssets_ttl_rsp), avg= mean(x.TotalAssets_ttl_rsp),q01=quantile(x.TotalAssets_ttl_rsp,0.01)
      ,q02=quantile(x.TotalAssets_ttl_rsp,0.02),q1=quantile(x.TotalAssets_ttl_rsp,0.25),median=quantile(x.TotalAssets_ttl_rsp,0.5)
      ,q3=quantile(x.TotalAssets_ttl_rsp,0.75),q98=quantile(x.TotalAssets_ttl_rsp,0.98)))













#######################################################################################################
## Nov, 4th,2019
## left join existing customer info to get the target customerinfo
df = CSV.read("C:\\Users\\012790\\Desktop\\RSP\\RSP_cus_76k.csv",missingstrings =["NULL",""])

dfv1 = CSV.read("C:\\Users\\012790\\Desktop\\RSP\\rsp_cus_full.csv")

[names(df) eltypes(df)]
[names(dfv1) eltypes(dfv1)]

df[!,:join_ind] = [Date.(x)>=Date("2018-12-01") ? "dec af" :
                   Date.(x)>=Date("2019-03-15") ? "af mar 15" : "dec 01 to march 15" for x in df[!,:join_date]]
freqtable(df[!,:join_ind])

t1=dfv1[dfv1.target .== 1,[:PrimaryClientID, :target]]

dff=join(df,t1, on=:PrimaryClientID , kind= :left)

CSV.write("C:\\Users\\012790\\Desktop\\RSP\\RSP_cus_76k_v2.csv",dff)

#######################################
df=dff

[names(df) eltypes(df)]

freqtable(df[!,:target],df[!,:join_ind])

df[!,:EquityCAD_dec18] = [ismissing(x) ? 0 : x for x in df[!,:EquityCAD_dec18]]
df[!,:EquityCAD_mar19] = [ismissing(x) ? 0 : x for x in df[!,:EquityCAD_mar19]]
df[!,:equity_diff] = df[!,:EquityCAD_mar19] - df[!,:EquityCAD_dec18]

## df[!,:effect] = df[!,:EquityCAD_dec18] + df[!,:EquityCAD_mar19] + df[!,:equity_diff] + df[!,:TotalAssets_ttl_rsp] + df[!,:txn_time_rsp]
## df[!,:effec_ind] = [x==0 ? "remove" : "keep" for x in df[!,:effect]]
## final ind for all cusotmer with rsp
##df[!,:final_ind] =[ismissing(x)&(y=="dec af") ? "net new cus with rsp" : ismissing(x)&(y=="dec bf") ? "exist cus already rsp" : "exist create rsp in season"
##                   for (x,y) in zip(df[!,:target],df[!,:join_ind])]


## quick info about equity and fund diff
t1 = by(df,[:final_ind,:effec_ind], [:equity_diff] =>
      x-> (N = length(x.equity_diff), avg= mean(x.equity_diff),ttl=sum(x.equity_diff),q01=quantile(x.equity_diff,0.01)
            ,q02=quantile(x.equity_diff,0.02),q1=quantile(x.equity_diff,0.25),median=quantile(x.equity_diff,0.5)
            ,q3=quantile(x.equity_diff,0.75),q98=quantile(x.equity_diff,0.98)))

t1 = by(df,:final_ind, [:TotalAssets_ttl_rsp] =>
      x-> (N = length(x.TotalAssets_ttl_rsp), avg= mean(x.TotalAssets_ttl_rsp),ttl=sum(x.TotalAssets_ttl_rsp),q01=quantile(x.TotalAssets_ttl_rsp,0.01)
      ,q02=quantile(x.TotalAssets_ttl_rsp,0.02),q1=quantile(x.TotalAssets_ttl_rsp,0.25),median=quantile(x.TotalAssets_ttl_rsp,0.5)
      ,q3=quantile(x.TotalAssets_ttl_rsp,0.75),q98=quantile(x.TotalAssets_ttl_rsp,0.98)))
