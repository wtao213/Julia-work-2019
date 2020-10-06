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

gr()



#####################################################################
# import data
df = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\customer_journey_20oct\\cus_full_479k_1001.csv",missingstrings=["","NULL"]))

dff = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\customer_journey_20oct\\cus_survival_480k.csv",missingstrings=["","NULL"]))


[names(df) eltype.(eachcol(df))]



df[!,:join_yr] = year.(df[!,:join_date])
freqtable(df[!,:join_yr])

#######################################################################
# check for survival rate
summarystats(dff[!,:act_act_t1])

# survival clients after 1 month
length(dff[(dff.MTD .>= 1),:act_act_t1])
length(dff[(dff.MTD  .>= 1).&(dff.act_act_t1 .> 0),:act_act_t1])


# survival clients after 2 month
length(dff[(dff.MTD  .>= 2),:tenure])
length(dff[(dff.MTD  .>= 2).&(dff.act_act_t2 .> 0),:tenure])

# survival clients after 3 month
length(dff[(dff.MTD  .>= 3),:tenure])
length(dff[(dff.MTD  .>= 3).&(dff.act_act_t3 .> 0),:tenure])

# survival clients after 6 month
length(dff[(dff.MTD  .>= 6),:tenure])
length(dff[(dff.MTD  .>= 6).&(dff.act_act_t6 .> 0),:tenure])

# survival clients after 12 month
length(dff[(dff.MTD  .>= 12),:tenure])
length(dff[(dff.MTD  .>= 12).&(dff.act_act_t12 .> 0),:tenure])


# survival clients after 15 month
length(dff[(dff.MTD  .>= 15),:tenure])
length(dff[(dff.MTD  .>= 15).&(dff.act_act_t15 .> 0),:tenure])


# survival clients after 18 month
length(dff[(dff.MTD  .>= 18),:tenure])
length(dff[(dff.MTD  .>= 18).&(dff.act_act_t18 .> 0),:tenure])


# survival clients after 21 month
length(dff[(dff.MTD  .>= 21),:tenure])
length(dff[(dff.MTD  .>= 21).&(dff.act_act_t21 .> 0),:tenure])


# survival clients after 24 month
length(dff[(dff.MTD  .>= 24),:tenure])
length(dff[(dff.MTD  .>= 24).&(dff.act_act_t24 .> 0),:tenure])



## look at the distibtuion for cusotmer already left questrade
summarystats(dff[dff.act_acct .== 0,:tenure])

histogram(
      collect(skipmissing(dff[dff.act_acct .== 0,:tenure])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Tenure for Attired Distribution",
      xlabel = "Tenure by Month",
      ylabel = "Count of Attired Customers",
      xticks = 0:12:144,
)
plot!([median(skipmissing(dff[dff.act_acct .== 0,:tenure]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,yformatter = x->string(Int(x/1e3),"K")
      )





dff= nothing




##############################################################
# distribution

# age when join
summarystats(df[!,:age_join])
quantile(skipmissing(df[!,:age_join]),0.98)

# Age
histogram(
      collect(skipmissing(df[!,:age_join])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Age Distribution",
      xlabel = "Age when join",
      ylabel = "Count of Customer",
#      xticks = 0:50000:200000,
      nbins = 18:1:80,
)
plot!([median(skipmissing(df[!,:age_join]))], seriestype="vline", label="Median"
      ,linestyle = :dash
#      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )



# density plots
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








#######################
# look at tenure
summarystats(df[!,:MTD ])
quantile(skipmissing(df[!,:MTD ]),0.98)

# tenure
histogram(
      collect(skipmissing(df[!,:MTD ])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Tenure Distribution",
      xlabel = "Tenure by Month",
      ylabel = "Count of Customer",
      xticks = 0:12:120,
      nbins = 0:1:120,
)
plot!([median(skipmissing(df[!,:MTD ]))], seriestype="vline", label="Median"
      ,linestyle = :dash
#      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )





############################################################
#     Financial behavior
#
[names(df) eltype.(eachcol(df))]

x = 2018
x in (2019,2018)

freqtable(df[!,:join_yr])

df[!,:yr_ca] = [ x == 2020 ? "2020" : x in (2019,2018,2017) ? "17-19" : "16 and before" for x in df[!,:join_yr]]
freqtable(df[!,:yr_ca])




##
# equity
summarystats(df[!,:EquityInCAD_t1_avg])
quantile(collect(skipmissing(df[!,:EquityInCAD_t1_avg ])),[0.9,0.95,0.98])

# plot
histogram(
      collect(skipmissing(df[!,:EquityInCAD_t1_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Month Equity Distribution",
      xlabel = "First Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_t1_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )


#plot by groups
b = df[df.EquityInCAD_t1_avg .!== missing,:]
histogram(
      collect(skipmissing(b[!,:EquityInCAD_t1_avg])),
      groups = b[!,:yr_ca],
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "First Month Equity Distribution",
      xlabel = "First Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(b[!,:EquityInCAD_t1_avg]))], seriestype="vline", label="Median",linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
#      ,yformatter = x->string(Int(x/1e3),"K")
      )

#
histogram(
      collect(skipmissing(b[!,:EquityInCAD_t1_avg])),
      groups = b[!,:yr_ca],
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "First Month Equity Distribution",
      xlabel = "First Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(b[!,:EquityInCAD_t1_avg]))], seriestype="vline", label="Median",linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )






##
summarystats(df[!,:EquityInCAD_t2_avg])
quantile(collect(skipmissing(df[!,:EquityInCAD_t2_avg ])),[0.9,0.95,0.98])

# plot
histogram(
      collect(skipmissing(df[!,:EquityInCAD_t2_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Second Month Equity Distribution",
      xlabel = "Second Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_t2_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )



##
summarystats(df[!,:EquityInCAD_t3_avg])
quantile(collect(skipmissing(df[!,:EquityInCAD_t3_avg ])),[0.9,0.95,0.98])

# plot
histogram(
      collect(skipmissing(df[!,:EquityInCAD_t3_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Third Month Equity Distribution",
      xlabel = "Third Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_t3_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )




##
summarystats(df[!,:EquityInCAD_t6_avg])
quantile(collect(skipmissing(df[!,:EquityInCAD_t6_avg ])),[0.9,0.95,0.98])

# plot
histogram(
      collect(skipmissing(df[!,:EquityInCAD_t6_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Six Month Equity Distribution",
      xlabel = "Six Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_t6_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )



#
##
summarystats(df[!,:EquityInCAD_t12_avg])
quantile(collect(skipmissing(df[!,:EquityInCAD_t12_avg ])),[0.9,0.95,0.98])

# plot
histogram(
      collect(skipmissing(df[!,:EquityInCAD_t12_avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "12th Month Equity Distribution",
      xlabel = "12th Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:EquityInCAD_t12_avg]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )




## animate for the plot
# legend=:bottomleft

anim = Animation()
for i in [1,2,3,6,12]
      med = round(median(skipmissing(df[!,Symbol(string("EquityInCAD_t",i,"_avg"))])), digits =0)
      histogram(
            collect(skipmissing(df[!,Symbol(string("EquityInCAD_t",i,"_avg"))])),
            fillalpha = 0.4,
            linealpha = 0.1,
            label = "QT customers",
      #      legend = false,
            title = "$i Month Equity Distribution",
            xlabel = "$i Month Equity",
            ylabel = "Count of Customer",
            xticks = 0:5000:50000,
            nbins = 0:1000:50000,
      )
      plot!([median(skipmissing(df[!,Symbol(string("EquityInCAD_t",i,"_avg"))]))], seriestype="vline", label="Median = $med"
            ,linestyle = :dash
            ,xformatter = x->string("\$",Int(x/1e3),"K")
            ,yformatter = x->string(Int(x/1e3),"K")
            )
      frame(anim)
end

gif(anim,fps = 2)






##############################################################
# TotalAssets_ttl_before_t1
summarystats(df[!,:TotalAssets_ttl_before_t1])
quantile(collect(skipmissing(df[!,:TotalAssets_ttl_before_t1])),[0.9,0.95,0.98])


#
histogram(
      collect(skipmissing(df[!,:TotalAssets_ttl_before_t1])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Month Equity Distribution",
      xlabel = "First Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(df[!,:TotalAssets_ttl_before_t1]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )



#
#plot by groups
b = df[df.TotalAssets_ttl_before_t1 .!== missing,:]
histogram(
      collect(skipmissing(b[!,:TotalAssets_ttl_before_t1])),
      groups = b[!,:yr_ca],
      normalize = :pdf,
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "First Month Equity Distribution",
      xlabel = "First Month Equity",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
)
plot!([median(skipmissing(b[!,:TotalAssets_ttl_before_t1]))], seriestype="vline", label="Median",linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
#      ,yformatter = x->string(Int(x/1e3),"K")
      )



#############################
# trading volumne
# trade_time_first_t1

summarystats(df[!,:trade_time_first_t1])
quantile(collect(skipmissing(df[!,:trade_time_first_t1])),[0.9,0.95,0.98])

df[!,:trade_time_first_t1] = [ismissing(x) ? 0 : x  for x in df[!,:trade_time_first_t1]]

#
histogram(
      collect(skipmissing(df[!,:trade_time_first_t1])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "First Month Trading Distribution",
      xlabel = "First Month Trading volumne",
      ylabel = "Count of Customer",
      nbins = 0:1:20,
)
plot!([median(skipmissing(df[!,:trade_time_first_t1]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,yformatter = x->string(Int(x/1e3),"K")
      )


# trading by groups
histogram(
      collect(skipmissing(df[df.join_yr .>= 2016,:trade_time_first_t1])),
      fillalpha = 0.4,
      linealpha = 0.1,
      normalize = :pdf,
      groups = df[df.join_yr .>= 2016,:yr_ca],
      title = "First Month Trading Distribution",
      xlabel = "First Month Trading volumne",
      ylabel = "Pct of Customer",
      nbins = 0:1:20,
)
plot!([median(skipmissing(df[df.join_yr .>= 2016,:trade_time_first_t1]))], seriestype="vline", label="Median"
      ,linestyle = :dash
      ,yformatter = x->string(round(x*100,digits =0),"%")
      )


# it seems like no trading data before 2016
freqtable(df[df.join_yr .>= 2016,:trade_time_first_t1],df[df.join_yr .>= 2016,:yr_ca])
