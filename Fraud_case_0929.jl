#############################################################
# start date: Sep,29rd,2020
# look at the fraud cases to explore
#
#

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
using Missings

gr()


################################################################
# import data
df    = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\fraud\\fraud_cus_94_full0929.csv",missingstrings=["","NULL"]))
cus   = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\customer_journey_20oct\\cus_full_479k_1001.csv",missingstrings=["","NULL"]))

a     = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\fraud\\WealthScapes2019_PRCDDA_GEO.csv",missingstrings=["","NULL"]))
mat   = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\fraud\\ePCCF_match.csv",missingstrings=["","NULL"]))

[names(df) eltype.(eachcol(df))]
[names(cus) eltype.(eachcol(cus))]


## data manipulation
df[!,:EquityInCAD_q1_diff] = df[!,:EquityInCAD_q1_max] .- df[!,:EquityInCAD_q1_min]

cus[!,:TotalAssets_ttl_before_t3] = passmissing(sum(cus[!,:TotalAssets_ttl_before_t2_t3],cus[!,:TotalAssets_ttl_before_t1_t2]))

df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in df[!,:PostalCode]]


summarystats(df[!,:EquityInCAD_q1_diff])


## join tables to get your info
# only 93 out of 94 clients been found
full = innerjoin(df,mat, on = :PC_ref => :FSALDU)
full = innerjoin(a,full,on= :CODE=>:PRCDDA)

mat = nothing
a = nothing

CSV.write("C:\\Users\\012790\\Desktop\\fraud\\cus_93_full_EA.csv",full)

######################################################
# univariabte distribution
summarystats(df[!,:age_join])
quantile(skipmissing(df[!,:age_join]),0.98)

# age when join
histogram(
      collect(skipmissing(df[!,:age_join])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Age Distribution",
      xlabel = "Age when join",
      ylabel = "Count of Customer",
#      xformatter = x->string("\$",Int(x/1e3),"K"),
#      xticks = 0:50000:200000,
#      yformatter = x->string(Int(x/1000),"K"),
     nbins = 18:1:74,
)
plot!([median(skipmissing(df[!,:age_join]))], seriestype="vline", label="Median",linestyle = :dash)




#
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
plot!([median(skipmissing(df[!,:age_join]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:age_join]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash,
      yformatter = x->string(Int(x*100),"%"))






#########################################################
# MTD tenure
summarystats(df[!,:MTD])
quantile(skipmissing(df[!,:MTD]),0.98)

# equity
histogram(
      collect(skipmissing(df[!,:MTD])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Tenure Distribution",
      xlabel = "Tenure",
      ylabel = "Count of Customer",
#      xformatter = x->string("\$",Int(x/1e3),"K"),
#      xticks = 0:50000:200000,
#      yformatter = x->string(Int(x/1000),"K"),
     nbins = 0:5:125,
)
plot!([median(skipmissing(df[!,:MTD]))], seriestype="vline", label="Median",linestyle = :dash)




#############################################
# MTD tenure
summarystats(df[!,:trade_time_first_q])
quantile(skipmissing(df[!,:trade_time_first_q]),0.98)

# equity
histogram(
      collect(skipmissing(df[!,:trade_time_first_q])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Trading volume Distribution",
      xlabel = "first quarter trading volumne",
      ylabel = "Count of Customer",
#      xformatter = x->string("\$",Int(x/1e3),"K"),
#      xticks = 0:50000:200000,
#      yformatter = x->string(Int(x/1000),"K"),
#     nbins = 18:1:74,
)
plot!([median(skipmissing(df[!,:trade_time_first_q]))], seriestype="vline", label="Median",linestyle = :dash)



# compare version
histogram(
      collect(skipmissing(df[!,:MTD])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Tenure Distribution",
      xlabel = "Tenure by month",
      ylabel = "Percentage of Clients",
      label = "Fraud Clients",
      nbins = 0:2:150
)
histogram!(
      collect(skipmissing(cus[!,:MTD])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT Clients",
      xticks = 0:12:144,
      nbins = 0:2:150
)
plot!([median(skipmissing(df[!,:MTD]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:MTD]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash,
      yformatter = x->string(Int(x*100),"%"))








#############################################
# EquityInCAD_q1_avg
summarystats(df[!,:EquityInCAD_q1_avg])
quantile(skipmissing(df[!,:EquityInCAD_q1_avg]),0.98)

# equity
histogram(
      collect(skipmissing(df[!,:EquityInCAD_q1_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Equity Distribution",
      xlabel = "First Quarter Equity Average",
      ylabel = "Count of Customer",
#      xticks = 0:50000:200000,
      nbins = -5000:1000:30000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_q1_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash,xformatter = x->string("\$",Int(x/1e3),"K"))




# equity
# compare version
summarystats(df[!,:EquityInCAD_q1_avg])
summarystats(cus[!,:EquityInCAD_t3_avg])

# Please verify the percentile again
histogram(
      collect(skipmissing(df[!,:EquityInCAD_q1_avg])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Equity Distribution",
      xlabel = "Equity first quarter",
      ylabel = "Percentage of Clients",
      label = "Fraud Clients",
      nbins = -5000:1000:30000
)
histogram!(
      collect(skipmissing(cus[!,:EquityInCAD_t3_avg])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT Clients",
      nbins = -5000:1000:30000
)
plot!([median(skipmissing(df[!,:EquityInCAD_q1_avg]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:EquityInCAD_t3_avg]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash,
       xformatter = x->string("\$",Int(x/1e3),"K"),
       yformatter = x->string(round(x*100,digits=3),"%")
      )













####################################
# look at first 3 months asset difference, and equity difference
[names(df) eltype.(eachcol(df))]


# net asset in first quarter
summarystats(df[!,:TotalAssets_ttl_b_3])
quantile(skipmissing(df[!,:TotalAssets_ttl_b_3]),0.98)


histogram(
      collect(skipmissing(df[!,:TotalAssets_ttl_b_3])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Quarter Net Asset Movement Distribution",
      xlabel = "Net Asset in first quarter",
      ylabel = "Count of Customer",
      xticks = -10000:10000:60000,
      nbins = -10000:1000:60000,
)
plot!([median(skipmissing(df[!,:TotalAssets_ttl_b_3]))], seriestype="vline", label="Median"
      ,linestyle = :dash,xformatter = x->string("\$",Int(x/1e3),"K"))




# compare
histogram(
      collect(skipmissing(df[!,:TotalAssets_ttl_b_3])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Quarter Net Asset Movement Distribution",
      xlabel = "Net Asset in first quarter",
      ylabel = "Count of Customer",
      xticks = -10000:10000:60000,
      nbins = -10000:1000:60000,
)
histogram!(
      collect(skipmissing(cus[!,:EquityInCAD_t3_avg])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT Clients",
      nbins = -5000:1000:30000
)
plot!([median(skipmissing(df[!,:EquityInCAD_q1_avg]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:EquityInCAD_t3_avg]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash,
       xformatter = x->string("\$",Int(x/1e3),"K"),
       yformatter = x->string(round(x*100,digits=3),"%")
      )




##
# net asset in first quarter
summarystats(df[!,:EquityInCAD_q1_diff])
quantile(skipmissing(df[!,:EquityInCAD_q1_diff]),0.98)


histogram(
      collect(skipmissing(df[!,:EquityInCAD_q1_diff])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Quarter Net Asset Movement Distribution",
      xlabel = "Net Asset in first quarter",
      ylabel = "Count of Customer",
      xticks = 0:10000:60000,
      nbins = 0:1000:60000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_q1_diff]))], seriestype="vline", label="Median"
      ,linestyle = :dash,xformatter = x->string("\$",Int(x/1e3),"K"))









############################################################
# phase 2: look at fraud cases postal code, cell, marriage status
#           income vs ea data
[names(df) eltype.(eachcol(df))]

[names(cus) eltype.(eachcol(cus))]

df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in df[!,:PostalCode]]

# freqtable
sort(freqtable(df[!,:PostalCode]), rev = true)
sort(freqtable(df[!,:PC_ref]), rev = true)


sort(freqtable(df[!,:PostalCode]), rev = true)
sort(freqtable(df[!,:PC_ref]), rev = true)

sort(freqtable(df[!,:Income]), rev = true)
sort(freqtable(df[!,:LiquidAsset]), rev = true)
sort(freqtable(df[!,:NetWorth]), rev = true)

sort(freqtable(df[!,:MaritalStatus]), rev = true)
sort(freqtable(cus[!,:MaritalStatus]), rev = true)

sort(freqtable(df[!,:NetFixedAsset]), rev = true)


##
# Income
summarystats(df[!,:Income])
summarystats(cus[!,:Income])


histogram(
      collect(skipmissing(df[!,:Income])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "KYC Income Distribution",
      xlabel = "KYC Income",
      ylabel = "Percentage of Clients",
      label = "Fraud Clients",
      nbins = 0:5000:300000
)
histogram!(
      collect(skipmissing(cus[!,:Income])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT Clients",
      xticks =0:50000:300000,
      nbins = 0:5000:300000
)
plot!([median(skipmissing(df[!,:Income]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:Income]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yaxis=nothing
#       yformatter = x->string(round(x*100,digits=3),"%")
      )




# liquidasset
summarystats(df[!,:LiquidAsset])
summarystats(cus[!,:LiquidAsset])


histogram(
      collect(skipmissing(df[!,:LiquidAsset])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "KYC Liquid Asset Distribution",
      xlabel = "KYC Liquid Asset",
      ylabel = "Percentage of Clients",
      label = "Fraud Clients",
      nbins = 0:5000:500000
)
histogram!(
      collect(skipmissing(cus[!,:LiquidAsset])),
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "QT Clients",
      #xticks = 0:500000:1000000,
      nbins = 0:5000:500000
)
plot!([median(skipmissing(df[!,:LiquidAsset]))], seriestype="vline", label="Fraud Median"
      ,linestyle = :dash)
plot!([median(skipmissing(cus[!,:LiquidAsset]))], seriestype="vline", label="QT Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yaxis=nothing
#       yformatter = x->string(round(x*100,digits=3),"%")
      )



#
# liquidasset




####################################################################
# heatmap


#####################
# age join vs income
histogram2d(df[!,:age_join],df[!,:Income],
            nbins=25,
            xlabel="Age",ylabel="KYC Income",
            c=cgrad(:blues),
#            clims=(0,5),
            xticks =20:5:80,
            ylims=(0,300000),
            yticks =0:50000:300000,
            title="Fraud cases: Join Age vs. KYC Income")
plot!(yformatter = x->string("\$",Int(x/1e3),"K"))


# bench of qt clients
# do the selection before heatmap, if not, will exist julia
b = cus[(cus.age_join .!== missing) .& (cus.Income .!== missing) ,[:age_join,:Income]]
b = b[(b.age_join .<= 80) .& (b.age_join .>= 18) .& (b.Income .<= 300000) ,:]


histogram2d(b[!,:age_join],b[!,:Income],
            nbins=20,
            xlabel="Age",ylabel="KYC Income",
            c=cgrad(:blues),
            clims=(0,10000),
            xticks =20:5:80,
            yticks =0:50000:300000,
            title="QT clients: Join Age vs. KYC Income")
plot!(yformatter = x->string("\$",Int(x/1e3),"K"))

b= nothing



#####################
# liquidasset vs income
histogram2d(df[!,:LiquidAsset],df[!,:Income],
            nbins=25,
            xlabel="KYC Liquid Asset",ylabel="KYC Income",
            c=cgrad(:blues),
#            clims=(0,5),
#            xticks =20:5:80,
            xlims=(0,500000),
            ylims=(0,300000),
#            yticks =0:50000:300000,
            title="Fraud cases: KYC Liquid Asset vs. KYC Income")
plot!(yformatter = x->string("\$",Int(x/1e3),"K"),
      xformatter = x->string("\$",Int(x/1e3),"K"))









#########################################################################
# EA data validation
# liquid asset
full[!,:LIQASTAVG] = full[!,:WSLIQASTB] ./ full[!,:WSLIQASTI]
full[!,:LIQASTPEN] = full[!,:WSLIQASTI] ./ full[!,:WSHHDTOT]


# total Savings
full[!,:WSSAVNGAVG] = full[!,:WSSAVNGB] ./ full[!,:WSSAVNGI]
full[!,:WSSAVNGPEN] = full[!,:WSSAVNGI] ./ full[!,:WSHHDTOT]

# Liquid asset - non-RSP
full[!,:WSLIQORAVG] = full[!,:WSLIQORB] ./ full[!,:WSLIQORI]
full[!,:WSLIQORPEN] = full[!,:WSLIQORI] ./ full[!,:WSHHDTOT]




## full[!,:LIQASTAVG] in ea
summarystats(full[!,:LIQASTAVG])
summarystats(df[!,:LiquidAsset])



histogram(
      collect(skipmissing(full[!,:LIQASTAVG])),
#      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "KYC vs EA Liquid Asset Distribution",
      xlabel = "Liquid Asset",
      ylabel = "Count of Clients",
      label = "Fraud Clients EA",
      nbins = 0:5000:500000
)
histogram!(
      collect(skipmissing(full[!,:LiquidAsset])),
#      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "Fraud Clients KYC",
      nbins = 0:5000:500000
)
plot!(
      xformatter = x->string("\$",Int(x/1e3),"K"))










##########################################################
# 2020-10-09
# deal with cell
# this is all customer could find, 122 accounts info, different than above
# practise on regular expression
cell  = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\fraud\\Fraud_cell.csv",missingstrings=["","NULL"]))

[names(cell) eltype.(eachcol(cell))]

unique(cell[!,:PrimaryClientID])
freqtable(cell[!,:DaytimePhone])


#= cell situation
      1. have blanks
      2. have ()
      3. start with 1
      4. have -
      5. has extention  X....

=#
df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in df[!,:PostalCode]]

occursin(r"X|x","877-750-2350 X 101")

replace("(514) 679-5704 ",[' ','(',')','-']=>"")

a= findlast("X","877-750-2350 X 101")

"877-750-2350 X 101"[1:collect(a)[1]-1]

# don't use substring in this, otherwise will get result like "123" instead 123
new = [ismissing(x) ? missing : replace(x,[' ','(',')','-']=>"") for x in cell[!,:DaytimePhone]]
new = [ismissing(x) ? missing : occursin(r"^1",x) ? x[2:length(x)] : x for x in new]
new = [ismissing(x) ? missing : occursin(r"X|x",x) ? x[1:collect(findlast("X",uppercase(x)))[1]-1] : x for x in new]

sort(freqtable(new),rev =true)
