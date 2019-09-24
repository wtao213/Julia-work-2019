####################################################################
## try to write a full version of logistic regression code
## start date: Sep,24th,2019
##
## function want to take care of:
##      logit plot
##      Lack-of-Fit Test
##      Information gain
##      KS(Kolmogorov-Smirnov statistic) Somer's D value
##      roc curve/gini
##      scoring process  campaign cut-off
##      population Stability Index
##
#######################################################################


ENV["COLUMNS"]=240
ENV["LINES"] = 50

using CSV
using FreqTables
using StatsBase
using Plots
using DataFrames
using Tables
using CategoricalArrays
using StatsModels

## import a sample code
df =CSV.read("C:\\Users\\012790\\Desktop\\post_analytics\\client_asset_34k.csv")
df= df[[x in ["Onboarding Team","no invite"] for x in df[!,:Type]], :]

[names(df) eltypes(df)]


## logit plot
df[!,:target] = [ x=="Onboarding Team" ? 1 : 0 for x in df[!,:Type]]

freqtable(df[!,:target])



## part 1: logit plot
## sorting x and cut into n bins, find the mean of each bins
## floor(tiedrank(x)*k/n+1)




## scatter plot and line plot in one
## function for draw the logit plot, df is the original function,
## x is the variable on x axies, y is the target variable, k is the number indicate bins group
## logit formula y=log(p/(1-p))
## almost equal to log((sum_of_1_in_gourp + 1)/(count_of_group - sum_of_1_in_gourp + 1)
function logit_plot(df::DataFrame,x::Symbol,k::Integer,y::Symbol)

      df[!,:rank] =  ceil.(Int,tiedrank(df[!,x])*k/(length(df[!,x]) +1))
      df1 = by(df,:rank, target = y =>mean, a = x => mean, n= y=>length, cls = y => sum)
      df1[!,:logit] = [log( (c + 1)/ (d - c + 1) ) for (c,d) in zip(df1[!,:cls],df1[!,:n])]

      scatter(df1[!,:a],df1[!,:logit])
      plot!(df1[!,:a],df1[!,:logit])

end

logit_plot(df,:TotalAssets_ttl_15_90,10,:target)



## body part of logistic regression
