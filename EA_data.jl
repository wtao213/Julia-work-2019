## ###########################################################
# start date: June,15nd,2020
# given certain attributes, we could divide the attrrite in different groups
# then assigne score to each groups
# finally rank every customer by total score
# based on v3, we want to add in layer of EA data

ENV["COLUMNS"]=240
ENV["LINES"] = 100

using CSV
using FreqTables
using StatsBase
using Statistics
using Plots
using ODBC
using DataFrames
using StatsPlots
using Dates
using CategoricalArrays

gr()



#######################################
# import data from EA
ws  = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\WealthScapes2019_CY_GEO_v2.csv",missingstrings=["","NULL"])
mat = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\ePCCF_match.csv",missingstrings=["","NULL"])

cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_score_full_318k_sr_ver_3_0.csv",missingstrings=["","NULL"])

[names(ws) eltype.(eachcol(ws))]
[names(mat) eltype.(eachcol(mat))]
[names(mat) eltype.(eachcol(mat))]

#################################
# check freq level
# minimal level is PRCDDA
freqtable(ws[!,:GEO])


# get average and pen for ws
# WSLIQASTI	WealthScapes Liquid Assets - Incidence
# WSLIQASTB	WealthScapes Liquid Assets - Balance
# WSCHQSAVB	Chequing & Savings Accounts - Balance
# WSSAVNGB	Total Savings - Balance
# WSSAVNGI	Total Savings - Incidence
# WSCHQSAVI	Chequing & Savings Accounts - Incidence
# WSPRIMREI	Primary Real Estate - Incidence
# WSPRIMREV	Primary Real Estate - Value
# WSMORTI	Mortgage - Incidence
# WSMORTB	Mortgage - Balance
# WSLIQORI	WealthScapes Liquid Assets - Non-RSP - Incidence
# WSLIQORB	WealthScapes Liquid Assets - Non-RSP - Balance


# liquid asset
ws[!,:LIQASTAVG] = ws[!,:WSLIQASTB] ./ ws[!,:WSLIQASTI]
ws[!,:LIQASTPEN] = ws[!,:WSLIQASTI] ./ ws[!,:WSHHDTOT]

# Mortgatge
ws[!,:WSMORTAVG] = ws[!,:WSMORTB] ./ ws[!,:WSMORTI]
ws[!,:WSMORTPEN] = ws[!,:WSMORTI] ./ ws[!,:WSHHDTOT]

# Primary Real Estate
ws[!,:WSPRIMREAVG] = ws[!,:WSPRIMREV] ./ ws[!,:WSPRIMREI]
ws[!,:WSPRIMREPEN] = ws[!,:WSPRIMREI] ./ ws[!,:WSHHDTOT]

# total Savings
ws[!,:WSSAVNGAVG] = ws[!,:WSSAVNGB] ./ ws[!,:WSSAVNGI]
ws[!,:WSSAVNGPEN] = ws[!,:WSSAVNGI] ./ ws[!,:WSHHDTOT]

# Liquid asset - non-RSP
ws[!,:WSLIQORAVG] = ws[!,:WSLIQORB] ./ ws[!,:WSLIQORI]
ws[!,:WSLIQORPEN] = ws[!,:WSLIQORI] ./ ws[!,:WSHHDTOT]

CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\WealthScapes2019_CY_GEO_v2.csv",ws)
CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\WealthScapes2019_PRCDDA_GEO.csv",a)
# raw minuplate of postal code
freqtable(cus[!,:PostalCode])
freqtable(cus[!,:PC_ref])

cus[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\d\D\d$",x) ? uppercase(x) : missing
                 for x in cus[!,:PostalCode]]
##inner join to get PRCDDA level info
# PRCDDA with 56590 records
a = ws[ws.GEO .== "PRCDDA",:]
a[!,:CODE] = [parse(Int64,x) for x in a[!,:CODE]]

prcdda = innerjoin(a,mat,on= :CODE=>:PRCDDA)
ws=nothing


## super full data info with EA
df_full = leftjoin(cus,prcdda,on= :PC_ref=>:FSALDU)

[names(df_full) eltype.(eachcol(df_full))]

summarystats(df_full[!,:WSHHDTOT])
# customer's allocation of PRCDDA density, not that urban
quantile(skipmissing(df_full[!,:WSHHDTOT]),0:0.1:1)

CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_318k_superfull.csv",df_full)
#

CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_318k_superfull.csv",df)

########################################################################################
# analysys from here
# at PRCDDA level
df = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_318k_superfull.csv",missingstrings=["","NULL","NaNs"])

[names(df) eltype.(eachcol(df))]


sort(freqtable(df[!,:CSDNAMEE]),rev=true)
sort(freqtable(df[!,:CMANAMEE]),rev=true)
l = ["Toronto  ON","Vancouver  BC","Montréal  QC","Calgary  AB","Ottawa - Gatineau  ON/QC"]

df[!,:WSLIQORAVG] = df[!,:WSLIQORB] ./ df[!,:WSLIQORI]
df[!,:WSLIQORPEN] = df[!,:WSLIQORI] ./ df[!,:WSHHDTOT]


# nan is type number, not missing... need to change first then could apply
df[!,:WSLIQORAVG] = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSLIQORAVG]]
df[!,:LIQASTAVG]  = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:LIQASTAVG]]
df[!,:WSMORTAVG]  = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSMORTAVG]]
df[!,:WSPRIMREAVG]= [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSPRIMREAVG]]
df[!,:WSSAVNGAVG] = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSSAVNGAVG]]

b=df[df.CMANAMEE .!== missing,:]


## household distribution
summarystats(df[!,:WSHHDTOT])
quantile(collect(skipmissing(df[!,:WSHHDTOT])),0:0.1:1)
quantile(collect(skipmissing(df[!,:WSHHDTOT])),0.84)

# for customer live in area with large hh number
summarystats(b[b.WSHHDTOT .>= 1000,:WSHHDTOT])
sort(freqtable(b[b.WSHHDTOT .>= 1000,:CMANAMEE]),rev= true)

histogram(
      collect(skipmissing(df[!,:WSHHDTOT])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
#      legend = false,
      title = "Area Household Distribution",
      xlabel = "Household num in CDDA",
      ylabel = "Count of Customer",
      yformatter = x->string(Int(x/1e3),"K"),
      nbins = 0:100:4000,
)
plot!([median(skipmissing(df[!,:WSHHDTOT]))], seriestype="vline", label="Median",linestyle = :dash)


# canadian wise prcdda
summarystats(a[!,:WSHHDTOT])
quantile(a[!,:WSHHDTOT],0:0.1:1)

histogram!(
      a[!,:WSHHDTOT],
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "Canadian",
#      legend = false,
      title = "Postal Code Distribution",
      xlabel = "Customer num in a postal code",
      ylabel = "Count of postal code",
      yformatter = x->string(Int(x/1e3),"K"),
      nbins = 0:100:4000,
)





#####################################################
# WealthScapes check

# WSCHQSAVB	Chequing & Savings Accounts - Balance
# WSSAVNGB	Total Savings - Balance
# WSSAVNGI	Total Savings - Incidence
# WSCHQSAVI	Chequing & Savings Accounts - Incidence
# WSPRIMREI	Primary Real Estate - Incidence
# WSPRIMREV	Primary Real Estate - Value
# WSMORTI	Mortgage - Incidence
# WSMORTB	Mortgage - Balance
# WSLIQORI	WealthScapes Liquid Assets - Non-RSP - Incidence
# WSLIQORB	WealthScapes Liquid Assets - Non-RSP - Balance use this as downpayment proxcy


## df[!,:WSLIQORAVG]  Liquid Asset-Non-distribution
#df[!,:WSLIQORPEN]
summarystats(df[!,:WSLIQORAVG])
quantile(collect(skipmissing(df[!,:WSLIQORAVG])),[0.95,0.98,0.99])


histogram(
      collect(skipmissing(df[!,:WSLIQORAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
#      legend = false,
      title = "Liquid Asset Non-RSP Distribution",
      xlabel = "CDDA Liquid Asset Non-RSP Average",
      ylabel = "Count of Customer",
      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:100000:800000,
      nbins = 0:10000:800000,
)
plot!([median(skipmissing(df[!,:WSLIQORAVG]))], seriestype="vline", label="Median",linestyle = :dash)

# toronto only
summarystats(b[b.CMANAMEE .== "Toronto  ON",:WSLIQORAVG])
summarystats(b[b.CMANAMEE .== l[2],:WSLIQORAVG])

histogram(
      collect(skipmissing(b[b.CMANAMEE .== l[2],:WSLIQORAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
#      legend = false,
      title = "$(l[2]) Liquid Asset Non-RSP Distribution",
      xlabel = "$(l[2]) Liquid Asset Non-RSP Average",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:100000:800000,
      nbins = 0:10000:800000,
)
plot!([median(skipmissing(b[b.CMANAMEE .== l[2],:WSLIQORAVG]))], seriestype="vline", label="Median",linestyle = :dash)



## liquid asset distribution
# liquid asset
summarystats(collect(skipmissing(df[!,:LIQASTAVG])))
quantile(collect(skipmissing(df[!,:LIQASTAVG])),0:0.1:1)
summarystats(df[!,:LIQASTPEN])


histogram(
      collect(skipmissing(df[!,:LIQASTAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
#      legend = false,
      title = "Liquid Asset Distribution",
      xlabel = "CDDA Liquid Asset Average",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:200000:1500000,
      nbins = 0:10000:1500000,
)



## mortgage
ws[!,:WSMORTAVG] = ws[!,:WSMORTB] ./ ws[!,:WSMORTI]
ws[!,:WSMORTPEN] = ws[!,:WSMORTI] ./ ws[!,:WSHHDTOT]

summarystats(df[!,:WSMORTAVG])


# average mortage distribution
histogram(
      collect(skipmissing(df[!,:WSMORTAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Mortgage Distribution",
      xlabel = "CDDA Mortgage Average",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:200000:1500000,
      nbins = 0:10000:1500000,
)
plot!([median(skipmissing(df[!,:WSMORTAVG]))], seriestype="vline", label="Median",linestyle = :dash)

# Toronto
# mortage penetration
histogram(
            collect(skipmissing(b[b.CMANAMEE .== "Toronto  ON",:WSMORTAVG])),
            fillalpha = 0.4,
            linealpha = 0.1,
            label = "QT customer",
            legend = false,
            title = "Mortgage Distribution",
            xlabel = "CDDA Mortgage Average",
            ylabel = "Count of Customer",
      #      yformatter = x->string(Int(x/1e3),"K"),
            xformatter = x->string("\$",Int(x/1e3),"K"),
            xticks = 0:200000:1500000,
            nbins = 0:10000:1500000,
      )


# mortage penetration
histogram(
      collect(skipmissing(df[!,:WSMORTPEN])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Mortgage penatration Distribution",
      xlabel = "CDDA Mortgage Penetration",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
#      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:0.1:1,
      nbins = 0:0.01:1,
)




##  Primary Real Estate
# Primary Real Estate
summarystats(df[!,:WSPRIMREAVG])
quantile(collect(skipmissing(df[!,:WSPRIMREAVG])),0:0.1:1)


# average Primary Real Estate
histogram(
      collect(skipmissing(df[!,:WSPRIMREAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Primary Real Estate Distribution",
      xlabel = "CDDA Real Estate Average",
      ylabel = "Count of Customer",
      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:500000:3000000,
      nbins = 0:50000:3000000,
)
plot!([median(skipmissing(df[!,:WSPRIMREAVG]))], seriestype="vline", label="Median",linestyle = :dash)


# only toronto?
b=df[df.CMANAMEE .!== missing,:]
freqtable(b[!,:CMANAMEE])
b[b.WSPRIMREAVG .>= 1000000,:]
# only toronto real estate
summarystats(b[b.CMANAMEE .== l[2],:WSPRIMREAVG])

histogram(
#      collect(skipmissing(b[b.CMANAMEE .== "Toronto ON",:WSPRIMREAVG])),
      b[b.CMANAMEE .== l[2],:WSPRIMREAVG],
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "$(l[2]) Primary Real Estate Distribution",
      xlabel = "$(l[2]) Real Estate Average",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:500000:3000000,
      nbins = 0:50000:3000000,
)
plot!([median(collect(skipmissing(b[b.CMANAMEE .== l[2],:WSPRIMREAVG])))], seriestype="vline", label="Median",linestyle = :dash)



# overal pen
histogram(
      collect(skipmissing(df[!,:WSPRIMREPEN])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Real Estate penatration Distribution",
      xlabel = "CDDA Real Estate Penetration",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
#      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:0.1:1,
      nbins = 0:0.01:1,
)




## total Savings Distribution
# total Savings

b[b.WSSAVNGAVG .>= 200000,:]
b[b.WSSAVNGAVG .>= 100000,:]
summarystats(collect(skipmissing(df[!,:WSSAVNGAVG])))
quantile(collect(skipmissing(df[!,:WSSAVNGAVG])),0:0.1:1)
summarystats(b[b.CMANAMEE .== "Toronto  ON",:WSSAVNGAVG])

histogram(
      collect(skipmissing(df[!,:WSSAVNGAVG])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Total Saving Distribution",
      xlabel = "CDDA Total Saving Average",
      ylabel = "Count of Customer",
      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:100000:500000,
      nbins = 0:10000:500000,
)
plot!([median(skipmissing(df[!,:WSSAVNGAVG]))], seriestype="vline", label="Median",linestyle = :dash)

# sabing penetration
# overal pen
# canadian wise, mode is around 40k -80k
histogram(
      collect(skipmissing(df[!,:WSSAVNGPEN])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Total Saving penatration Distribution",
      xlabel = "CDDA Total Saving Penetration",
      ylabel = "Count of Customer",
#      yformatter = x->string(Int(x/1e3),"K"),
#      xformatter = x->string("\$",Int(x/1e3),"K"),
      xticks = 0:0.1:1,
      nbins = 0:0.01:1,
)



#### median household income of the CDDA area
histogram(
      collect(skipmissing(df[!,:WSMEINC])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT customer",
      legend = false,
      title = "Median HH Income Distribution",
      xlabel = "CDDA Median HH Income Average",
      ylabel = "Count of Customer",
      yformatter = x->string(Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"),
#      xticks = 0:100000:1000000,
#      nbins = 0:10000:1000000,
)




## insurability test
# df[!,:WSLIQORAVG]
# df[!,:LIQASTAVG]
# df[!,:WSMORTAVG]
# df[!,:WSPRIMREAVG]
# df[!,:WSSAVNGAVG]
[names(df) eltype.(eachcol(df))]

df[!,:downpay_20] = [ismissing(x) ? missing : x*0.1999 for x in df[!,:WSPRIMREAVG]]

df[!,:insurability] = [ismissing(x) ? "None" : x>= 1000000 ? "Not Insurable" : z<y ? "Insured" : "Insurable"
                        for (x,y,z) in zip(df[!,:WSPRIMREAVG],df[!,:downpay_20],df[!,:WSLIQORAVG])]


t = freqtable(df[!,:insurability])
prop(t)



freqtable(df[!,:rank_ver_3_0],df[!,:insurability])
