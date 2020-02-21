

## julia practise
## start date: July,22nd,2019

ENV["COLUMNS"]=240
ENV["LINES"] = 50

using CSV
using FreqTables
using StatsBase
using Statistics
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
by(df,[:TypeName,:STA_Status],(:Trade_day_num_30,:TypeName) =>
        x->(trade_mean=mean(skipmissing(x.Trade_day_num_30)), N = length(x.TypeName), N_missing = sum(ismissing.(x.Trade_day_num_30))))

by(full_v2,[:TypeName,:STA_Status],:Trade_day_num_30=> mean, :TypeName=>length)


## function using to count the distinct value in a column
length(unique(df[!,:AccountClass]))


## filter rows exist in a list of strings, string column has no missing
df2 = df[[x in ["IN","FX"] for x in df[!,:AccountClass]], :]
## if the column contain missing value, then doesn't work, have to add in filter to filter out missing, these three conditions' sequence doesn't matter
WM_WM = df[(df.ind_t12 .!== missing) .& (df.ind_t0 .== "WM") .& (df.ind_t12 .== "WM") , :]
df2= df[(df.rsp_check .== "first not RSP") .& (x in ["SD","WM","multi-class"] for x in df[!,:classtype]) ,:]


#####################################################
## different types of conversion
## convert string to date or union{Missing,Date}
## expression ? a : b  <=> if expression is true, then return a; if expression is false, then return b
datefm= dateformat"mm/dd/yyyy"
df[!,:completed_date]= Date.(df[!,:completed_date],datefm)
date = [ismissing(x) ? missing : Date(x,datefm) for x in df[!,:close_date]]

## convert string to num
parse(Int64,"123")
parse(Int64,"123",16)
parse(Float64,"123.123")

## convert num to string
string(123)



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

## prop(table,1/2) when 1 that's row pct, when 2 that column pct, when no number, that's total pct
t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status])
prop(t1,1)

## to only get a subset freqtable!!!!
## subset to numeric or categorical
t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status],subset=rsp[!,:rank_join] .< 5)
t1= freqtable(rsp[!,:rank_join],rsp[!,:STA_Status],subset=[x in ["Complete","Closed"] for x in rsp[!,:STA_Status]])

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

## tenory control on two seperate conditions
 nov_full[!,:new_result] = [ ismissing(x) &&ismissing(y) ? "no action" : "action"
                         for (x,y) in zip(nov_full[!,:trade_time],nov_full[!,:AssetIn_ttl_11])]






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






## using odbc to create temp table back to sql server
db=ODBC.DSN("Description=juslia;DRIVER=SQL Server;SERVER=corp-bi;Trusted_Connection=Yes";prompt=false)

## write back to sql server
stmt = ODBC.prepare(db, "INSERT INTO #Temp (Account_18_Digit, c_Type) VALUES(?, ?)")

for row in Tables.rows(list)
    ODBC.execute!(stmt, (row.Account_18_Digit, row.Type))
end

## get data from sql server
df=ODBC.query(db,"select ")




## scatter plot and line plot in one
## function for draw the logit plot, df is the original function,
## x is the variable on x axies, y is the target variable, k is the number indicate bins group
## logit formula y=log(p/(1-p))
## almost equal to log((sum_of_1_in_gourp + 1)/(count_of_group - sum_of_1_in_gourp + 1)

## x could be CategoricalArrays, add dispatch function of it, change from mean to median
## issue, what if x has missing
function logit_plot(df::DataFrame,x::Symbol,k::Integer,y::Symbol)

      df[!,:rank] =  ceil.(Int,tiedrank(df[!,x])*k/(length(df[!,x]) +1))
     ## df1 = by(df,:rank, target = y =>mean, a = x => mean, n= y=>length, cls = y => sum)
     df1 = by(df,:rank, target = y =>mean, a = x => median, n= y=>length, cls = y => sum)
      df1[!,:logit] = [log( (c + 1)/ (d - c + 1) ) for (c,d) in zip(df1[!,:cls],df1[!,:n])]

      scatter(df1[!,:a],df1[!,:logit],xlabel=x,ylabel="logit target")
      ## plot!(df1[!,:a],df1[!,:logit]) ## this is not loess line!
end

logit_plot(df,:TotalAssets_ttl_15_90,20,:target)





## sampling dataframe
myDF = DataFrame(A = 1:10, B = 21:30)
myDF[sample(axes(myDF, 1), 3; replace = false, ordered = true), :]

## get quantile of the column
quantile(cus_oct_v2[!,:MTD],(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1))
nquantile(cus_oct_v2[!,:MTD],10)



## data transpose, from long to wide
## unstack(df,:row,:col,:value)
trade=unstack(df2,:PrimaryClientID,:time_ind,:trade_time)



## try heatmap, and change colorgradient
##plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=20
##      ,c=ColorGradient([:green,:yellow,:blue]))
## :blues , :viridis, :magma , :ingerno, :plasma, :'color's (:reds,:greens etc)
plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=25,xlabel="Age",ylabel="Tenure",
            c=ColorGradient(:blues),title="Age vs. Tenure")

## how to limit your x, y axis,
plot(y, xlabel = "my label",
                xlims = (0,10),
                xticks = 0:0.5:10,
                xscale = :log,
                xflip = true,
                xtickfont = font(20, "Courier"),
                legend = false)


##get a subset of the dataframe, and dropping all rows with missing
a = cus_oct_v2[:, [:trade_max_q, :equity_max_after2016]]
a = a[completecases(a), :]
## for df, only remove the columns age_dec18 's missing value, still return whole df'
b = df[completecases(df,:age_dec18),:]
completecases(df, [:x, :y])


### to calculate the mean at centain quantile range
quantile(cus_oct_v2[!,:trade_passing_yr],0.98)
mean(skipmissing([x<=47 ? x : missing for x in cus_oct_v2[!,:trade_passing_yr]]))


p1 = histogram(
    no_charge[!, :MTD],
    fillalpha = 0.4,
    linealpha = 0.1,
    title = "No charge",
    xlabel = "Month",
    ylabel = "Client Count",
    nbins = 0:1:155,
    legend = false,
)


p2 = histogram(
    full_chrage[!, :MTD],
    fillalpha = 0.4,
    linealpha = 0.1,
    title = "full charge",
    xlabel = "Month",
    ylabel = "Client Count",
    nbins = 0:1:155,
    legend = false,
)
p3 = histogram(
    partial_charge[!, :MTD],
    fillalpha = 0.4,
    linealpha = 0.1,
    title = "partial charge",
    xlabel = "Month",
    ylabel = "Client Count",
    nbins = 0:1:155,
    legend = false,
)
plot(p1, p2, p3, layout = (3, 1), size = (600, 800), tickfontsize = 6)






## format your x-axix label!!! by xformatter
## "\u0024" is the unicode of dollar sign
## string("\u0024",14,"K")
histogram(act[!,:EquityInCAD],fillalpha=0.4,linealpha=0.1, nbins=0:2500:150000
 ,xticks = 0:10000:150000
,xformatter = x->string("\u0024",Int(x/1e3),"K")
,xlabel="Asset",ylabel="Customer Count",legend=false)


## sum all columns, or sum all rowa
A = [1 2; 3 4]
# sum all values in a column
sum(A, 1)
# sum all columns value in a row
sum(A, 2)



## name the dafaframe columns
names!(df, [Symbol.(:X, 1:4); Symbol.(:Y, 1:4)])

## look at all columns contain "2" in the column names
 df[:, r".2"]


## calculation about dates
dfv1[!,:a] = [Date.(x)>=Date("2017-12-01") ? "dec af" : "dec bf" for x in dfv1[!,:join_date]]




#### cutomize plots
plot(y, xlabel = "my label",
    xlims = (0,10),
    xticks = 0:0.5:10,
    xscale = :log,
    xflip = true,
    xtickfont = font(20, "Courier"))



#################################################
## ROC_C(area under roc) and Somer's D (Gini index)
Somers_D = 2*ROC_c - 1

## add loess line to the plot
using Loess
using Gadfly
using Plots

## import function from certain package when there are conflict
import Gadfly.plot

x_data = 0.0:0.1:2.0
y_data = x_data.^2 + rand(length(x_data))
plot(x=x_data, y=y_data, Geom.point, Geom.smooth(method=:loess,smoothing=0.9))






#########################
## change missingvalue to certain value
collect(Missings.replace(df_full[!,:TotalAssets_ttl_t1],0))
## remove the missing value
collect(skipmissing(df_full[!,:TotalAssets_ttl_t1]))





###########################
## condistional filter to remove the duplicate in dataframe
by(df, :A) do sbdf
    (size(sbdf, 1)>1) ? sbdf[sbdf.B.==1,:] : sbdf
end



#####################################
## sorting dataframe on multiple columns
sort!(df, [:a, :x])
collect(1:3)
