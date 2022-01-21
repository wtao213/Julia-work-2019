#######################################################
# start: 2021-05-03
# look at how many trade line each client ever have
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
using DelimitedFiles

gr()


##############################################

######################################################################
# read file
tc  = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\Equifax_file1.csv",missingstrings=["","NULL"]))
[names(tc) eltype.(eachcol(tc))]



##############
# aggregate by clients
ai_df = groupby(tc,:custref)
ai_df= combine(ai_df) do x
      (N            = length(x.custref)
      ,emember_n    = length(unique(skipmissing(x.member)))
      ,dtreport_min = minimum(x.dtreport)
      ,dtreport_max = maximum(x.dtreport)
      ,highcr_max      = maximum(x.highcr)
      ,highcr_min      = minimum(x.highcr)

      )
end



######
# freqtable
tb=freqtable(ai_df[!,:N])


CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\tradeline_agg_20210503.csv",ai_df)