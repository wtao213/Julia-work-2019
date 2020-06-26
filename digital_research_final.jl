## ##################################
# Digital research budget forcast
# start date: 2020-05-20

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


##################################################################
# data import
df = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\digital.csv")

search      = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\search.csv")
paid_search = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\paid_search.csv")
non_clinet_search = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\non_client_search.csv")

[names(df) eltype.(eachcol(df))]

[names(search ) eltype.(eachcol(search ))]
[names(paid_search) eltype.(eachcol(paid_search))]




###########################################################################
# data manipulate
# date manipulate, right 4 digit is year before first / is month, convert to num
paid_search[!,:year]  = [parse(Int,x[end-3:end]) for x in paid_search[!,:MonthStart]]
paid_search[!,:month] = [parse(Int,x[1:findfirst('/',x)-1]) for x in paid_search[!,:MonthStart]]


search_full = leftjoin(paid_search,non_clinet_search, on = [:Campaign,:MonthStart], makeunique = true)

search_full[!,:Sessions_client] = search_full[!,:Sessions] - search_full[!,:Sessions_1]
search_full[!,:Users_client]    = search_full[!,:Users] - search_full[!,:Users_1]

[names(search_full) eltype.(eachcol(search_full))]


# merge with the active customer info dataframe df
# paid_search_f = innerjoin(df,pd_search_w,on=[:year,:month]) innerjoin to avoid missing
# the one we are looking at are the clients session only
search_full_w   = unstack(search_full, [:year,:month],:Campaign,:Sessions_client, renamecols=x->Symbol(x,"_Sessions"))
search_full_w2  = unstack(search_full, [:year,:month],:Campaign,:Users_client, renamecols=x->Symbol(x,"_Users"))

search_full_w = innerjoin(search_full_w,search_full_w2, on = [:year,:month])

search_full_w2 = nothing



## merge with customer base
paid_search_f = innerjoin(df,search_full_w,on=[:year,:month])


[names(paid_search_f) eltype.(eachcol(paid_search_f))]

#####################################################
# plotting
#
# passmissing() vs. skipmissing()
# animate it
# handle missing solve your problem


## plot for month and year
anim = Animation()
plot(
     paid_search_f[!, :cutomer_count]
    ,paid_search_f[! ,:Branded_Sessions])
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
        ,ylabel = "Branded Sessions Clinet only")
    frame(anim)
end

gif(anim,fps = 2)



############################################################
# for VIX and DJ delta, I want to see overall session, vs branded session
pd_search_w   = unstack(paid_search, [:year,:month],:Campaign,:Sessions, renamecols=x->Symbol(x,"_Sessions"))
pd_search_w2  = unstack(paid_search, [:year,:month],:Campaign,:Users, renamecols=x->Symbol(x,"_Users"))

pd_search_w = innerjoin(pd_search_w,pd_search_w2, on = [:year,:month])

pd_search_w2 = nothing

[names(pd_search_w) eltype.(eachcol(pd_search_w))]

# total sessions
pd_search_w[!,:total_Sessions] = pd_search_w[!,:Branded_Sessions] + pd_search_w[!,:Competitors_Sessions]+ pd_search_w[!,:Others_Sessions] + pd_search_w[!,:Unbranded_Sessions]

head(pd_search_w,10)
##
# merge with other info
paid_search_f2 = innerjoin(df,pd_search_w,on=[:year,:month])

[names(paid_search_f) eltype.(eachcol(paid_search_f))]


## look at search vs VIX

#         ,fillalpha = 0.4, linealpha = 0.1,legend = false
summarystats(paid_search_f2[!,:Vix_close])

anim = Animation()
for (i,j) in zip(paid_search_f2[!,:year],paid_search_f2[!,:month])
    plot(
         paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :Vix_close]
        ,paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :Branded_Sessions]
        ,seriestype = :scatter
        ,legend = :topleft
        ,label = "Branded Sessions"
        ,ylims  = (0,800000)
        ,yticks = 0:100000:800000
        ,xlims  = (0,60)
        ,xticks = 0:10:60
        ,yformatter = x->string(Int(x/1e3),"K")
        ,title = "Year $i Month $j"
        ,xlabel = "Vix"
        ,ylabel = "Sessions")
    plot!(
         paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :Vix_close]
        ,paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :total_Sessions]
        ,seriestype = :scatter
        ,label = "Total Sessions")
    frame(anim)
end

# setup your gif name, auto change path to current foler
gif(anim,"vix_sessions.gif", fps = 2)



##
# look at search vs DJ delta
#         ,fillalpha = 0.4, linealpha = 0.1,legend = false

anim = Animation()
for (i,j) in zip(paid_search_f2[!,:year],paid_search_f2[!,:month])
    plot(
         paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :DJ_delta]
        ,paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :Branded_Sessions]
        ,seriestype = :scatter
        ,label = "Branded Sessions"
        ,ylims  = (0,800000)
        ,yticks = 0:100000:800000
        ,xlims  = (-6000,2000)
        ,xticks = -6000:2000:20000
        ,yformatter = x->string(Int(x/1e3),"K")
        ,title = "Year $i Month $j"
        ,xlabel = "Dow Jones Delta"
        ,ylabel = "Sessions")
    plot!(
         paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :DJ_delta]
        ,paid_search_f2[(paid_search_f2.year .== i).& (paid_search_f2.month .== j), :total_Sessions]
        ,seriestype = :scatter
        ,label = "Total Sessions")
    frame(anim)
end

# setup your gif name, auto change path to current foler
gif(anim,"DJdelta_sessions.gif", fps = 2)
















###############################################################################
# profit per lead

# first version 71664 accounts, v2 has 71469 accounts
#df = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\digital_transfer_0101_0608_96k.csv")
df = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\digital_transfer_1805_1904_72k_v2.csv")
[names(df) eltype.(eachcol(df))]

# forgot to ban 1 year or more
df[!,:cus_days] = 365 .- df[!,:convert_days]
df[!,:cus_month] = [round(x/30) for x in df[!,:cus_days]]

freqtable(df[!,:cus_month])
first(df[df.cus_month .< 0,:],100)

summarystats(df[!,:AmountInCAD])


## aggregate to customer level
# 51885 to 51847 after clear up
a=groupby(df[!,:],:PrimaryClientID)
a = combine(a) do x
      (act_num      = length(x.PrimaryClientID)
      ,profit       = sum(x.AmountInCAD)
      ,tenure       = maximum(x.MTD)
      ,cus_month    = maximum(x.cus_month)
      ,convert_days = minimum(x.convert_days)
      )
end


## profit distribution
# before 528 mean, now 432.8
freqtable(a[!,:tenure ])
summarystats(a[!, :profit])
quantile(a[!, :profit],[0.90,0.95,0.98,0.99])

a[!,:cus_month] = Int.(a[!,:cus_month])

a[!,:profit_index] = [x>0 ? "positive" : "negative or 0" for x in a[!, :profit]]
freqtable(a[!,:profit_index])
freqtable(a[!,:profit_index],a[!,:cus_month])

## profit distribtuion by tenure
# by how long being customer in their first year after created
histogram(
    a[!, :profit],
    group = a[!,:cus_month],
    fillalpha = 0.4,
    linealpha = 0.1,
#    legend = false,
    title = "Profit by Active months distribution",
    xlabel = "Profit",
    ylabel = "Client Count",
    nbins = -2000:200:10000,
    yformatter = x -> string(Int(x / 1e3), "K"),
    xformatter = x -> string("\$", Int(x / 1e3), "K"),
    xticks = -2000:1000:7000,
)
plot!([median(a[!, :profit])], seriestype="vline", label="Median",linestyle = :dash)




## lead convert distribution plot
# there are 53901 account are rank_join = 1
summarystats(df[df.rank_join .== 1, :convert_days])
quantile(df[df.rank_join .== 1, :convert_days],[0.95,0.98,0.99])
quantile(df[df.rank_join .== 1, :convert_days],0:0.1:1)

# convert day distribution
histogram(
    df[df.rank_join .== 1, :convert_days],
    fillalpha = 0.4,
    linealpha = 0.1,
    legend = false,
    title = "Convert distribution",
    xlabel = "Convert Days",
    ylabel = "Client Count",
    nbins = 0:1:200,
 #  xformatter = x -> string("\$", Int(x / 1e3), "K"),
    xticks = 0:20:200,
)
plot!([median(df[df.rank_join .== 1, :convert_days])], seriestype="vline", label="Median",linestyle = :dash)
