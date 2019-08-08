
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
## if the column contain missing value, then doesn't work, have to add in filter in begining to filter out missing
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
