


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
using Dates


gr()


## read the fulll version
df= CSV.read("C:\\Users\\012790\\Desktop\\notebook\\Julia\\full_clean_489.csv")

## if need to indicate your missing value
df2= CSV.read("C:\\Users\\012790\\Desktop\\cus_journey\\asset_full_clean.csv",missingstring="NULL")

## check your data type and data names
[names(df) eltype.(eachcol(df))]



CSV.write("C:\\Users\\012790\\Desktop\\fraud\\cus_93_full_EA.csv",full)

## data manipulation
# replace and modify cell
# replace NaN to missing, NaN due to one info incomplete
df3[!,:WSINVEIR_potential]=[ismissing(x) ? x : isnan(x) ? repalce(x,NaN => missing) : x for x in df3[!,:WSINVEIR_potential]]
replace!(df3[!,:WSSAVNOS_potential], NaN=>missing)

replace("(514) 679-5704 ",[' ','(',')','-']=>"")

# replace certain range to a value
replace(x -> x>300000 ? 300000 : x, df[!,:Income])



# filter all columns with same surffix
df_extra2= df_extra[!,filter(x -> occursin.(r"^Equity",x), names(df_extra))]
df_extra2= df_extra[!,["PrimaryClientID"; filter(x -> occursin.(r"^Equity",x), names(df_extra));"trade_t_3"]]


#= cell situation
      1. have blanks

      2. have ()
      3. start with 1
      4. have -
      5. has extention  X....

=#

# don't use substring in this, otherwise will get result like "123" instead 123
cell_cus[!,:cell_modify] = [ismissing(x) ? missing : replace(x,['*','+',' ','(',')','-']=>"") for x in cell_cus[!,:DaytimePhone]]
cell_cus[!,:cell_modify] = [ismissing(x) ? missing : occursin(r"^1",x) ? x[2:length(x)] : x for x in cell_cus[!,:cell_modify]]
cell_cus[!,:cell_modify] = [ismissing(x) ? missing : occursin(r"X|x",x) ? x[1:collect(findlast("X",uppercase(x)))[1]-1] : x for x in cell_cus[!,:cell_modify]]
# keep only numers
cell_cus[!,:cell_modify] = [ismissing(x) ? missing : occursin(r"\D",x) ? replace(x,r"\D"=>"") : x for x in cell_cus[!,:cell_modify]]
cell_cus[!,:cell_modify] = [ismissing(x) ? missing : occursin(r"^\d{10}$",x) ? x : missing for x in cell_cus[!,:cell_modify]]

sort(freqtable(cell_cus[!,:cell_modify]),rev =true)


cell_cus[!,:area_code] = [ismissing(x) ? missing : x=="" ? missing : x[1:3] for x in cell_cus[!,:cell_modify]]
sort(freqtable(cell_cus[!,:area_code]),rev =true)


cell_cus[!,:area_aggregate] = [ismissing(x) ? missing :
                              x in ("416","437","647") ? "416/437/647" :
                              x in ("403","587","825") ? "403/587/825" :
                              x in ("519","226","548") ? "519/226/548" :
                              x in ("418","367","581") ? "418/367/581" :
                              x in ("778","250","236") ? "778/250/236" :
                              x in ("905","289","365") ? "905/289/365" :
                              x in ("613","343") ? "613/343" :
                              x in ("514","438") ? "514/438" : x
                              for x in cell_cus[!,:area_code]]
sort(freqtable(cell_cus[!,:area_aggregate]),rev =true)





# want to get the rank within each group
df2 = combine(groupby(df,:age_t0),sdf -> sort(sdf,:Income), s->(rank=1:nrow(s),), nrow => :n)
# or using transform
df2 =transform(groupby(df,:age_t0), sdf -> sort(sdf,:Income), s->(rank=1:nrow(s),), nrow => :n)




## transpose the data frame to nuew way
trade_per_client = unstack(ai_df, :PrimaryClientID, :TradeDate2, :N_trade_s)



# fill missing to 0 for the whole dataframe
for col in eachcol(trade_per_client)
       replace!(col, missing=>0)
end





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

# new way to do by under dataframe
ai_df = groupby(cus,:age_today)
ai_df= combine(ai_df) do x
      (age_avg       = mean(x.age_today)
      ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
      ,act_avg      = mean(x.act_acct)
      ,tenure       = mean(x.MTD)
      ,N            = length(x.act_acct)
      ,tading       = mean(skipmissing(x.trade_time_lq))
      ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
      ,NetWorth     = median(skipmissing(x.NetWorth))
      ,median_Income       = median(skipmissing(x.Income))
      ,Income_10       = quantile(skipmissing(x.Income),0.10)
      ,Income_25       = quantile(skipmissing(x.Income),0.25)
      ,Income_75       = quantile(skipmissing(x.Income),0.75)
      ,Income_90       = quantile(skipmissing(x.Income),0.90)
      ,Income_98       = quantile(skipmissing(x.Income),0.98)
      )
end


# group by age bin
age_cuts = [20,25,30,35,40,45,50,55,60,65]
dff.age_ca = cut(dff.age_today, age_cuts, extend = true)
freqtable(dff.age_ca)
levels(dff.age_ca)


ai_df = groupby(dff,:age_ca)
ai_df= combine(ai_df) do x
      (age_avg       = mean(x.age_today)
      ,equity_avg   = mean(skipmissing(x.Equity0925))
      ,act_avg      = mean(x.act_acct)
      ,tenure       = mean(x.MTD)
      ,N            = length(x.act_acct)
      ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
      ,NetWorth     = median(skipmissing(x.NetWorth))
      ,median_Income   = median(skipmissing(x.Income))
      ,Income_10       = quantile(skipmissing(x.Income),0.10)
      ,Income_25       = quantile(skipmissing(x.Income),0.25)
      ,Income_75       = quantile(skipmissing(x.Income),0.75)
      ,Income_90       = quantile(skipmissing(x.Income),0.90)
      ,Equity0925_median   = median(skipmissing(x.Equity0925))
      ,Equity0925_10       = quantile(skipmissing(x.Equity0925),0.10)
      ,Equity0925_25       = quantile(skipmissing(x.Equity0925),0.25)
      ,Equity0925_75       = quantile(skipmissing(x.Equity0925),0.75)
      ,Equity0925_90       = quantile(skipmissing(x.Equity0925),0.90)
      ,LiquidAsset_median   = median(skipmissing(x.LiquidAsset))
      ,LiquidAsset_10       = quantile(skipmissing(x.LiquidAsset),0.10)
      ,LiquidAsset_25       = quantile(skipmissing(x.LiquidAsset),0.25)
      ,LiquidAsset_75       = quantile(skipmissing(x.LiquidAsset),0.75)
      ,LiquidAsset_90       = quantile(skipmissing(x.LiquidAsset),0.90)
      ,Liabilities_median   = median(skipmissing(x.Liabilities))
      ,Liabilities_10       = quantile(skipmissing(x.Liabilities),0.10)
      ,Liabilities_25       = quantile(skipmissing(x.Liabilities),0.25)
      ,Liabilities_75       = quantile(skipmissing(x.Liabilities),0.75)
      ,Liabilities_90       = quantile(skipmissing(x.Liabilities),0.90)
      ,NetWorth_median   = median(skipmissing(x.NetWorth))
      ,NetWorth_10       = quantile(skipmissing(x.NetWorth),0.10)
      ,NetWorth_25       = quantile(skipmissing(x.NetWorth),0.25)
      ,NetWorth_75       = quantile(skipmissing(x.NetWorth),0.75)
      ,NetWorth_90       = quantile(skipmissing(x.NetWorth),0.90)
      ,median_TCMG031   = median(skipmissing(filter(x -> x>0, x.TCMG031)))  #only work on certain range values
      )
end



using StatsBase

fh = fit(Histogram, [n1; n2], nbins=80)



# plot by Liabilities by categorical Array
plot(String.(ai_df[!,:age_ca]), ai_df[!,:Liabilities_90],  label = ("Liabilities 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:200000:2000000
      ,title=" Liabilities Trajectory"
      ,xlabel="Age",ylabel="Liabilities"
      ,legend=:topleft
      )
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Liabilities_median],  label = ("Liabilities Median"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Liabilities_10],      label = ("Liabilities 10%"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Liabilities_25],      label = ("Liabilities 25%"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Liabilities_75],      label = ("Liabilities 75%"))


# plot by equity
plot(String.(ai_df[!,:age_ca]), ai_df[!,:Equity0925_10],  label = ("Equity 10%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:30000:300000
      ,title=" Investment Trajectory"
      ,xlabel="Age",ylabel=" Equity0925"
      ,legend=:topleft
      )
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Equity0925_25],  label = ("Equity 25%"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Equity0925_median],      label = ("Equity median"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Equity0925_75],      label = ("Equity 75%"))
plot!(String.(ai_df[!,:age_ca]), ai_df[!,:Equity0925_90],      label = ("Equity 90%"))


#
# look at distribution
freqtable(df[!,:close_type])
summarystats(df[!,:tenure])

histogram(
      df[!,:tenure],
      fillalpha = 0.4,
      linealpha = 0.1,
#      legend = false,
      normalize = :pdf,
      group =  df[!,:close_type],
      title = "Tenure Distribution",
      xlabel = "Tenure by month",
      ylabel = "Percentage of Customer",
      xticks = 0:12:48,
#      yformatter = x->string(Int(x/1000),"K"),
      nbins = 0:1:50,
)
plot!([median(df[df.close_type .== "Attrited",:tenure])], seriestype="vline"
      ,label="Attried Med = $(median(df[df.close_type .== "Attrited",:tenure]))"
      ,linestyle = :dash)
plot!([median(df[df.close_type .== "swap",:tenure])], seriestype="vline"
      ,label="swap Med = $(median(df[df.close_type .== "swap",:tenure]))"
      ,linestyle = :dash)
plot!([median(df[df.close_type .== "concentrated",:tenure])], seriestype="vline"
      ,label="concentrated Med = $(median(df[df.close_type .== "concentrated",:tenure]))"
      ,linestyle = :dash
      ,yformatter = x->string(round(x*100,digits =0),"%"))



# histogram

histogram(
      collect(skipmissing(df[!,:EquityInCAD_q1_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Equity Distribution",
      xlabel = "first quarter Equity Average",
      ylabel = "Count of Customer",
      xformatter = x->string("\$",Int(x/1e3),"K"),
#      xticks = 0:50000:200000,
#      yformatter = x->string(Int(x/1000),"K"),
      nbins = -5000:1000:30000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_q1_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash,xformatter = x->string("\$",Int(x/1e3),"K"))


# density histogram, plot as percentage

histogram(
      collect(skipmissing(df[!,:age_join])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Age Distribution",
      xlabel = "Age when join",
      ylabel = "percentage of client of Customer",
#      xticks = 0:50000:200000,
      nbins = 18:1:80,
)



############
# density plot comparision
# compare version

histogram(
      collect(skipmissing(df[!,:age_join])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Age Distribution",
      xlabel = "Age when join",
      ylabel = "Percentage of Clients",
      label = "Fraud clients",
      nbins = 18:1:80

)
histogram!(
      collect(skipmissing(cus[!,:age_join])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT clients",
      nbins = 18:1:80
)
plot!([median(skipmissing(cus[!,:age_join]))],
      yformatter = x->string(Int(x*100),"%"))






################################################################################
# change specific column name
rename!(dff, Dict(:PS_final => "PS_final_25"))




## function using to count the distinct value in a column
length(unique(df[!,:AccountClass]))


## filter rows exist in a list of strings, string column has no missing
df2 = df[[x in ["IN","FX"] for x in df[!,:AccountClass]], :]
## if the column contain missing value, then doesn't work, have to add in filter to filter out missing, these three conditions' sequence doesn't matter
WM_WM = df[(df.ind_t12 .!== missing) .& (df.ind_t0 .== "WM") .& (df.ind_t12 .== "WM") , :]
df2= df[(df.rsp_check .== "first not RSP") .& (x in ["SD","WM","multi-class"] for x in df[!,:classtype]) ,:]
df[in.(df[!,:SIN], (l,)), :]



df[in(["Forex","Margin"]).(df.cus_type3), :age_today]
# for not in certain values
b = df[(df.province .!== missing) .& (x ∉ ["Quebec","Northwest Territories","Nunavut","Yukon"] for x in df[!,:province]),[:rank_ver_3_0,:insurability]]

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

# sort dataframe by a column
sort(a,:N,rev=true)

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



# regular expression to detect and replace
df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in df[!,:PostalCode]]


 ## ranking function  floor(tiedrank(x)*k/n+1) n is siae of non-missing value, k is the group uou use.
 df1[!,:asset_rank] = floor.(Int,  tiedrank(df1[!,:TotalAssets_ttl_t12])*10/(length(df1[!,:TotalAssets_ttl_t12]) +1))




 ## function to replace proc rank
 ## 1. indicate your group number
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end
# 2. indicate the interval you want
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

# delete a column by name
select!(df, Not(:count))

df[:, All(r"x", :)]

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

## update april 28, 2020 ColorGradient changed to cgrad
plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=25,xlabel="Age",ylabel="Tenure",
            c=cgrad(:blues),title="Age vs. Tenure")

#
histogram2d(dff[!,:Equity_t0]
            ,dff[!,:EquityInCAD_t_3_diff]
            ,nbins=10
            ,xlabel="Equity T0"
            ,ylabel="EquityInCAD_t_3_diff"
            ,c=cgrad(:blues)
            ,clims=(0, 5000)
            ,title="Equity vs. EquityInCAD_t_3_diff")
plot!(xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string("\$",Int(x/1e3),"K"))



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




## a very customized version of historgram
histogram(collect(skipmissing(df[(df.cus_seg .== "best"),:EquityCAD09avg])),
      fillalpha = 0.4, linealpha = 0.1,
    title = "Equity for best",  xlabel = "Equity" ,ylabel = "Client Count"
   ,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:400000
   ,nbins = 0:5000:400000,legend = false)



##
#
histogram(
      df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE],
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "FICO Score Distribution",
      label =" FICO score",
      xlabel = "FICO SCORE",
      ylabel = "Count of Customer",
      xticks = 400:50:900,
      legend =:topleft
#      nbins = -5000:1000:30000,
)
plot!([median(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE]))], seriestype="vline"
     , label="Median = $(median(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE])))"
      ,linestyle = :dash
      ,yformatter = x->string(Int(x/1e3),"K"))
plot!([quantile(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE]),0.1)]
     , seriestype="vline", label="Bottom 10% = $(quantile(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE],0.1))"
      ,linestyle = :dash)


#
# look at df[!,:TCAM031]
Med = string("\$",round(median(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "All Trade lines Balance Distribution",
      label = "All Balance",
      xlabel = "All Trade lines Balance",
      ylabel = "Count of Customer",
      xticks = 0:50000:500000,
      nbins = 0:5000:500000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )
plot!([median(skipmissing(df[!,:Equity_t0]))]
      ,seriestype="vline", label="Median =\$$(round(Int,median(skipmissing(df[!,:Equity_t0]))/1e3))K"
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1000),"K")
      ,linestyle = :dash)

#########################################
# add second y axis to the plot need package Measures
# add second axis
plot(df2[!,:yr_mth],df2[!,:acct_closing]
      ,marker=([:hex :d])
      ,markersize =2
      ,markeralpha = 0.5
      ,legend = :topleft
      ,yformatter = x->string(round(Int,x/1000),"K")
      ,label = "Closing Account"
      )
plot!(twinx(), df2[!,:act_ct]
      ,marker=([:hex :d])
      ,color=:red
      ,markersize =2
      ,markeralpha = 0.5
      ,legend = :topright
      ,yformatter = x->string(round(Int,x/1000),"K")
      ,label = "Active Account"
      ,ylabel = "Active Account Count"
      )
plot!(title   = "Closing Account per Month"
      ,xlabel = "Year Month"
      ,ylabel = "Close Account Count"
      ,right_margin =12mm
      ,xticks=(17)
      ,xrotation = 30)
savefig("C:\\Users\\012790\\Desktop\\attrition_2021\\close_vs_active.png")





## ploting on subset of the dataframe
# clims=(-0.005,0.005) for color scale control
histogram2d(
      df[(df.cus_seg.=="best"), :Overall_satisfy],
      df[(df.cus_seg.=="best"), :refer_friends],
      nbins = 25,
      xlabel = "Overall Satisfy",
      ylabel = "refer friends",
      c = ColorGradient(:blues),
      title = "Overall Satisfy vs. refer friends",
)

## sum all columns, or sum all rowa
A = [1 2; 3 4]
# sum all values in a column
sum(A, 1)
# sum all columns value in a row
sum(A, 2)


## months between different dates
length(start_date:Month(1):end_date)


## find the row contain certain string in a list of strings
l=["CIBC","SCOTIA","TDCT","NATIONAL","RBC","ROYAL","TD CANADA TRUST","BMO","BANK OF MONTREAL","HSBC"]
HELOC[(sum(occursin.(l, x)) for x in HELOC[!,:member]) .> 0,:]


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



## checking ploting bining
summarystats(RSP[!,:RSP_fundsin_inseason])






## want to have cut point at 25,30,40,50,65

## quantile cust
## age_cuts = quantile(df.age_today,[0.25,0.5,0.75])
## age_cuts = [-Inf; unique(quantile(df.age_today, 0.25:0.25:0.75)); Inf]
## absolute cut
##
age_cuts = [25,30,40,50,65]
df.age_ca = cut(df.age_today, age_cuts, extend = true)
freqtable(df.age_ca)
levels(df.age_ca)

## assign score to different level
## df[!,:age_score] = [x=="[30, 40)" ? 10 : x in ("[25, 30)","[40, 50)") ? 8 :
##                  x in ("[14, 25)","[50, 65)") ? 5 : 0  for x in df[!,:age_ca]]

## method 2: using map function to transfer your data,easy and clean to see
mycode = Dict(
              "[14, 25)" => 5,
              "[25, 30)" => 8,
              "[30, 40)" => 10,
              "[40, 50)" => 8,
              "[50, 65)" => 5,
              "[65, 101]"=> 0,
                ##      "" => missing,
                  )
df[!,:age_score]= [get(mycode, s, missing) for s in df.age_ca]

trade_cuts =[minimum(skipmissing(df[!,:trade_time_lq])),1,5,10,30,maximum(skipmissing(df[!,:trade_time_lq]))]


## delete a column by name
delete!(df,:trade_ca)
select!(df,Not(:trade_ca)) ## new version

## remove column 3
df = df[:,[1:2,4:end]]


















## animate plots works version
anim = Animation()
for i in 2018:2020
    bar(df[df.year .== i,:month],df[df.year .== i,:cutomer_count]
        ,fillalpha = 0.4, linealpha = 0.1,legend = false
        ,ylims  = (0,400000)
        ,yticks = 0:50000:400000
        ,xlims  = (0,12)
        ,xticks = 1:1:12
        ,yformatter = x->string(Int(x/1e3),"K")
        ,title = "Year $i Distribution"
        ,nbins = 1:1:12)
    frame(anim)
end

gif(anim,fps = 0.5)



## add number to a exsit vector
gain_cuts = append!([minimum(skipmissing(cus[!,:equity_pct_change]))],collect(-1:0.1:1))
gain_cuts = append!(gain_cuts,[maximum(skipmissing(cus[!,:equity_pct_change]))])












##########################
# get a substring
s= "N2N1W1"

SubString("N2N1W1", 2)
SubString("N2N1W1", 1,5)
SubString("N2N1W1", 2:4)
chop("N2N1W1", head=0, tail=2)
s[1:5]


df[!,:Postal_l5] = [ ismissing(x) ? missing : chop(strip(x),head=0,tail=1) for x in df[!,:PC_ref]]

## before do anything, check your postalcode formal
df[!,:PC_size] = sizeof.(df[!,:PostalCode])

maximum(sizeof.(df[!,:PostalCode]))
minimum(sizeof.(df[!,:PostalCode]))


# check the postalcode meet format and standize the letter
# \w could be num and word, \D is non dig
occursin(r"^\D\d\D\d\D\d$","a1d3f4")
r"^\D\d\D\d\D\d$"

df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\d\D\d$",x) ? uppercase(x) : missing
                 for x in df[!,:PostalCode]]







##
function Anonymize_Check(df::DataFrame, vars::Vector)
      test  = groupby(df, vars)
      test2 = combine(test, :PrimaryClientID =>length => :cus_count)

      count_values_of_allvar = length(test2[test2.cus_count .> 1,:cus_count])
      new_row_count          = sum(test2[test2.cus_count .> 1,:cus_count])
      suppressed_row_count   = sum(test2[test2.cus_count .== 1,:cus_count])

      avg_risk = new_row_count == 0 ? "--" : count_values_of_allvar/new_row_count

      println("""Anoymize Check
      suppressed rows count = $suppressed_row_count
      new rows count        = $new_row_count
      Average Risk          = $avg_risk
      """)
end
Anonymize_Check(df,[:age_today ,:MTD])








###########
## date manipulation

## date manipulate, right 4 digit is year
paid_search[!,:year]  = [parse(Int,x[end-3:end]) for x in paid_search[!,:MonthStart]]
paid_search[!,:month] = [parse(Int,x[1:findfirst('/',x)-1]) for x in paid_search[!,:MonthStart]]



# dataframe long to wide
pd_search_w   = unstack(paid_search, [:year,:month],:Campaign,:Sessions, renamecols=x->Symbol(x,"_Sessions"))




##
# passmissing() vs. skipmissing()
# animate it
# handle missing solve your problem
anim = Animation()
plot(
     paid_search_f[!, :cutomer_count]
    ,paid_search_f[! , :Branded_Sessions])
for (i,j) in zip(paid_search_f[!,:year],paid_search_f[!,:month])
    plot!(
         paid_search_f[(paid_search_f.year .== i).& (paid_search_f.month .== j), :cutomer_count]
        ,paid_search_f[(paid_search_f.year .== i).& (paid_search_f.month .== j), :Branded_Sessions]
        ,seriestype = :scatter
        ,fillalpha = 0.4, linealpha = 0.1,legend = false
        ,ylims  = (0,800000)
        ,yticks = 0:100000:800000
        ,xlims  = (250000,400000)
        ,xticks = 250000:50000:400000
        ,yformatter = x->string(Int(x/1e3),"K")
        ,xformatter = x->string(Int(x/1e3),"K")
        ,title = "Year $i Month $j"
        ,xlabel = "Active Customer"
        ,ylabel = "Branded Sessions")
    frame(anim)
end

gif(anim,fps = 2)





####################
# two ways to find all matchs for position detect
findall(r"\w+\s+",s[1]) # this return a range

getfield.(collect(eachmatch(r"\w+\s+", s[1])), [:offset]) # this return a list of start point



df = DataFrame()
for i in 1:length(p_s)
    df[!,c_n[i]]= string.(SubString.(s,p_s[i],p_e[i]))
end
df[!,:other] = string.(SubString.(s,last(p_e)+1))


occursin(r"\d+", df[1,2])
occursin(r"\d+", df[1,1])




s= readlines("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\EFX_File3", keep=true)
s =[x[1:270] for x in s]









###############################
#
v =[1,2,3,4,5,6,7,8,9,10]

accumulate(*,v)
accumulate(+,v)


# look at the column have number only
df[dftypes ,<: Numeric]
