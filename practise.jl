
## julia practise
## start date: July,22nd,2019

ENV["COLUMNS"]=240
ENV["LINES"] = 50

using CSV
using FreqTables
using StatsBase
using Plots
using ODBC
using DataFrames
using JLD2
using Dates
gr()


## read the fulll version
df= CSV.read("C:\\Users\\012790\\Desktop\\notebook\\Julia\\full_clean_489.csv")

## if need to indicate your missing value
df2= CSV.read("C:\\Users\\012790\\Desktop\\cus_journey\\asset_full_clean.csv",missingstring="NULL")

## check your data type and data names
[names(df) eltypes(df)]



## Try to write one function  return all the results
## add dispatch for a list of cols
## elseif  eltype(full[!,i]) == Union{Missing, Date} || eltype(full[!,i]) ==  Date
##    continue this syntax continue here is using to skip this situation
function col_check(full)
    for i in 1:size(full,2)
        if (eltype(full[!,i]) == String || eltype(full[!,i]) == Union{Missing, String}) && length(unique(df[!,i])) > 10
           println(names(full)[i]," has more than 10 categories")
           println()
            
        elseif eltype(full[!,i]) == String || eltype(full[!,i]) == Union{Missing, String}
            println("Column Name:",names(full)[i])  ## shows out your column name
            println(freqtable(full[!,i]))
            println() 
            
        elseif  eltype(full[!,i]) == Union{Missing, Date} || eltype(full[!,i]) ==  Date
            println("Column Name:",names(full)[i]," is a Date column")
            println()

        elseif eltype(full[!,i]) !==String
               println("Column Name:",names(full)[i])  ## shows out your column name
               println(summarystats(full[!,i]))
               println()
        end
    end
end

col_check(full_v2)

## filter data are not missing
full=full_v2[ full_v2[:AssetsOut_ttl_before].!== missing, :] ## filter data are not missing


## by(df, :a, (:b, :c) => x -> (minb = minimum(x.b), sumc = sum(x.c)))
## using this way to skip missing when do different calculation!!!! the default one couldn't use to filter missingvalue
by(df,[:TypeName,:STA_Status],(:Trade_day_num_30,:TypeName) => x->(trade_mean=mean(skipmissing(x.Trade_day_num_30)), N = length(x.TypeName), N_missing = sum(ismissing.(x.Trade_day_num_30))))

by(full_v2,[:TypeName,:STA_Status],:Trade_day_num_30=> mean, :TypeName=>length)


## function using to count the distinct value in a column
length(unique(df[!,:AccountClass]))


## filter rows exist in a list of strings, string column has no missing
df2 = df[[x in ["IN","FX"] for x in df[!,:AccountClass]], :]
## if the column contain missing value, then doesn't work, have to add in filter to filter out missing, these three conditions' sequence doesn't matter
WM_WM = df[(df.ind_t12 .!== missing) .& (df.ind_t0 .== "WM") .& (df.ind_t12 .== "WM") , :]




## convert string to date or union{Missing,Date}
## expression ? a : b  <=> if expression is true, then return a; if expression is false, then return b
datefm= dateformat"mm/dd/yyyy"
df[!,:completed_date]= Date.(df[!,:completed_date],datefm)
date = [ismissing(x) ? missing : Date(x,datefm) for x in df[!,:close_date]]



## original three way test
test(x, y) = println(x < y ? "x is less than y"    :
                     x > y ? "x is greater than y" : "x is equal to y")


## alternative short if statements
## if  <cond> <statement> end <=> <cond> && <statement>
## if !<cond> <statement> end <=> <cond> || <statement>

## this ternary operator somehow could help us convert from numberical to different groups , could add many different level as you need
clients_f[!,:gain_loss_ind] = [ ismissing(x) ? "missing" : x>0 ? "gain" : "loss" for x in clients_f[!,:gain_loss]]
freqtable(clients_f[!,:gain_loss_ind])

a = [ ismissing(x) ? "missing" : x <0 ? "neg" : x<1000 ? "0-1000" : "large" for x in clients_f[!,:gain_loss]]





## conversion
## type 1: numberic to string
df1[!,:AccountNumber] = [string(x) for x in df1[!,:AccountNumber]]
## type 2: string convert to numberic. Convert x to a value of type T. T is integer type, string to date works as well.
convert(T, x)



## check how to use a function
methods(disallowmissing!)
?disallowmissing!

## check the size of a DataFrame
axes(df)   # results (Base.OneTo(251882), Base.OneTo(26))
size(df)   # (251882, 26)

## plotting
histogram(df[!,:tenure],group=df[!,:STA_Status],fillalpha=0.4,linealpha=0.1,nbins=0:1:102,title="Account Tenure",xlabel="month",ylabel="account Count")



## be carefule when use mutli-dimention arry for loop
a = ["a","b","c"]
b = ["a","b","c","d"]
c= [x == y ? "euqal" : "not" for x in a,y in b]

## results
3×4 Array{String,2}:
 "euqal"  "not"    "not"    "not"
 "not"    "euqal"  "not"    "not"
 "not"    "not"    "euqal"  "not"

 ## using zip to make them in pairs
 a = ["a","c","b","c"]
 b = ["a","b","c","d"]
 c= [x == y ? "euqal" : "not" for (x,y) in zip(a,b)]

 # results 
 "euqal"
 "not"  
 "not"  
 "not" 


 ## ranking function  floor(tiedrank(x)*k/n+1) n is siae of non-missing value, k is the group uou use.
 df1[!,:asset_rank] = floor.(Int,  tiedrank(df1[!,:TotalAssets_ttl_t12])*10/(length(df1[!,:TotalAssets_ttl_t12]) +1))
 ## function to replace proc rank
 ## 1. indicate your group number
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end
## 2. indicate the interval you want
function rank(x::AbstractVector,p::AbstractFloat)
    0< p <=1 || error("p must between 0 to 1")
isinteger(1/p) || error("need ratio be exact divided by 1")
ceil.(Int,tiedrank(x)/(p*(length(x)+1)))
end

rank(df1[!,:TotalAssets_ttl_t12],10)







## Sep,24th,2019
## function for logit plot

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

logit_plot(df,:TotalAssets_ttl_15_90,20,:target)

