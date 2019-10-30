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

function logit_plot(df::DataFrame,x::Symbol,k::Integer,y::Symbol)

      df[!,:rank] =  ceil.(Int,tiedrank(df[!,x])*k/(length(df[!,x]) +1))
      df1 = by(df,:rank, target = y =>mean, a = x =>mean, n= y=>length, cls = y => sum)
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
