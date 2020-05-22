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

#############################
# import the file
df = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\digital.csv")

[names(df) eltype.(eachcol(df))]
## testing for animation plots

bar(df[df.year .== 2019,:month],df[df.year .== 2019,:cutomer_count])




## fps = plot per second
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
        ,nbins = 1:1:12
        ,xlabel = "Month"
        ,ylabel = "Active customer count")
    frame(anim)
end

gif(anim,fps = 0.5)


#############################################################################
# real work start from here
df = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\digital.csv")
search      = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\search.csv")
paid_search = CSV.read("C:\\Users\\012790\\Desktop\\Digital_research\\paid_search.csv")

[names(df) eltype.(eachcol(df))]

[names(search ) eltype.(eachcol(search ))]
[names(paid_search) eltype.(eachcol(paid_search))]


## date manipulate, right 4 digit is year before first / is month, convert to num
paid_search[!,:year]  = [parse(Int,x[end-3:end]) for x in paid_search[!,:MonthStart]]
paid_search[!,:month] = [parse(Int,x[1:findfirst('/',x)-1]) for x in paid_search[!,:MonthStart]]


# transpose the dataframe
# is there anyway to long to wide multiple columns? not only one?

pd_search_w   = unstack(paid_search, [:year,:month],:Campaign,:Sessions, renamecols=x->Symbol(x,"_Sessions"))
pd_search_w2  = unstack(paid_search, [:year,:month],:Campaign,:Users, renamecols=x->Symbol(x,"_Users"))

pd_search_w = innerjoin(pd_search_w,pd_search_w2, on = [:year,:month])

pd_search_w2 = nothing

[names(pd_search_w) eltype.(eachcol(pd_search_w))]

# total sessions
pd_search_w[!,:total_Sessions] = pd_search_w[!,:Branded_Sessions] + pd_search_w[!,:Competitors_Sessions]
                        + pd_search_w[!,:Others_Sessions] + pd_search_w[!,:Unbranded_Sessions]




# merge with the active customer info dataframe df
# paid_search_f = leftjoin(df,pd_search_w,on=[:year,:month])
paid_search_f = innerjoin(df,pd_search_w,on=[:year,:month])


[names(paid_search_f) eltype.(eachcol(paid_search_f))]
####################
# plotting

plot(
    paid_search_f[!, :cutomer_count],
    paid_search_f[!, :Branded_Sessions],
    seriestype = :scatter,
    title = "My Scatter Plot",
)

plot(
    [1],
    [2],
    seriestype = :scatter,
    title = "My Scatter Plot",
)

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

typeof(paid_search_f[(paid_search_f.year .== 2019).&(paid_search_f.month .== 2), :cutomer_count])

a= [(x,y) for (x,y) in zip(paid_search_f[!,:year],paid_search_f[!,:month])]



## this easy version works
anim = Animation()
plot(
     paid_search_f[!, :cutomer_count]
    ,paid_search_f[! , :Branded_Sessions])
for i in 1:12
    plot!(
         paid_search_f[paid_search_f.month .== i , :cutomer_count]
        ,paid_search_f[paid_search_f.month .== i , :Branded_Sessions]
        ,seriestype = :scatter
        ,fillalpha = 0.4, linealpha = 0.1,legend = false
        ,ylims  = (0,800000)
        ,yticks = 0:100000:800000
        ,xlims  = (250000,400000)
        ,xticks = 250000:50000:400000
        ,yformatter = x->string(Int(x/1e3),"K")
        ,xformatter = x->string(Int(x/1e3),"K")
        ,title = " Month $i"
        ,xlabel = "Active Customer"
        ,ylabel = "Branded Sessions")
    frame(anim)
end

gif(anim,fps = 2)





#=
    1.search clicks/money vs cusomer
=#
