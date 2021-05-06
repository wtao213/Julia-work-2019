#######################################################
# start: 2021-02-18
# we want to look at the information on TCHE,TCIN TCMG
# look at Equifax raw data

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

eqfx_dir = "C:/Users/012790/Desktop/Equifax_Customer_Data"
eqfx_fff5 = joinpath(eqfx_dir, "23930_CBM.PM.VTP.QUE17046.D201124.FUN50_Customer FFF.GLUE.out1")

const seglength = Dict{AbstractString, Int}(
    "FULL" => 245,
    "CA" => 105,
    "FA" => 105,
    "F2" => 105,
    "AK" => 75,
    "FN" => 75,
    "DT" => 15,
    "ES" => 110,
    "EF" => 110,
    "E2" => 110,
    "EC" => 110,
    "CF" => 110,
    "OI" => 60,
    "BP" => 125,
    "CO" => 150,
    "FM" => 125,
    "LI" => 260,
    "FO" => 65,
    "NR" => 20,
    "MI" => 150,
    "TL" => 120,
    "FC" => 45,
    "GN" => 238,
    "TC" => 135,
    "NT" => 80,
    "CS" => 110,
    "FB" => 70,
    "FI" => 50,
    "LO" => 55,
    "IQ" => 55,
    "CD" => 420,
    "BS" => 35,
    "PN" => 80,
    "&&" => 2,
)




#---    Functions

"""
f5_tc: process fff5 file for TC segments
"""
function f5_tc(file::AbstractString)
    out = DataFrame(
        custref  = AbstractString[],
        joint    = Char[],                  #
        member   = AbstractString[],
        dtreport = AbstractString[],
        dtopen   = AbstractString[],
        highcr   = Int[],
        balance  = Int[],
        typecode = Char[],                  # I R M
    )

    cnt = 0
    for line in eachline(file)
        cnt += 1
        read_tc!(out, line)
    end
    println("Records read: ", cnt)

    return out
end

"""
read_tc: parse a fff5 record for TC segments
"""
function read_tc!(df, f5rec)
    rec = SubString(f5rec, 1)

    # must start with FULL header
    segtype = SubString(rec, 1, 4)
    seglen = seglength[segtype]
    if segtype != "FULL"
        println("ERROR: record must start with FULL: ", str)
        return
    end

    custref = rstrip(rec[5:16])
    rec = chop(rec, head = seglen, tail = 0)
    while rec != ""
        segtype = SubString(rec, 1, 2)
        seglen = seglength[segtype]

        if segtype == "TC"
            joint    = rec[4]
            member   = rstrip(rec[6:25])            # categorical
            dtreport = rec[[52,53,54,55,50,51]]     # YYYYMM
            dtopen   = rec[[58,59,60,61,56,57]]     # YYYYMM
            shighcr  = rec[62:65]                   # can end in K or M
            sbalance = rec[70:73]                   # can end in K or M
            typecode = rec[78]                      # I R O M

            highcr = amount(shighcr)
            balance = amount(sbalance)

            push!(df,
                (custref, joint, member, dtreport, dtopen,
                 highcr, balance, typecode, )
             )
        end

        rec = chop(rec, head = seglen, tail = 0)
    end

    return
end

function amount(s::AbstractString)
    if s[end] == 'K'
        amt = parse(Int, s[1:end-1]) * 1000
    elseif s[end] == 'M'
        amt = parse(Int, s[1:end-1]) * 1000000
    else
        amt = parse(Int, s)
    end

    amt
end

#---

tc = f5_tc(eqfx_fff5)
Base.summarysize(tc)        # Total size in Bytes


CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\Equifax_file1.csv",tc)


######################################################################
# read file
tc  = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\Equifax_file1.csv",missingstrings=["","NULL"]))
[names(tc) eltype.(eachcol(tc))]


sort(freqtable(tc[!,:joint]),rev=true)
sort(freqtable(tc[!,:member]),rev=true)
sort(freqtable(tc[!,:typecode]),rev=true)

# HELOC is revolving
sort(freqtable(tc[tc.typecode .== 'R' ,:member]),rev=true)
sort(freqtable(tc[(tc.typecode .== 'R') .& (tc.highcr .> 10000) ,:member]),rev=true)
sort(freqtable(tc[(tc.typecode .== 'R') .& (tc.highcr .> 50000) ,:member]),rev=true)



sort(freqtable(tc[tc.typecode .== 'I' ,:member]),rev=true)
sort(freqtable(tc[tc.typecode .== 'I' ,:member]),rev=true)

sort(freqtable(tc[tc.typecode .== 'M',:member]),rev=true)

sort(freqtable(tc[(tc.typecode .!== 'M') .& (tc.highcr .> 50000) ,:typecode]),rev=true)


# calculate the records last report date to end date
# apply to whole data base take too long, please split and then calulate later
end_date = Dates.Date("202011", "yyyymm")

tc[!,:dtreport] = Dates.Date.(tc[!,:dtreport], "yyyymm")
tc[!,:rec_tot0] = [length(x:Month(1):end_date) for x in tc[!,:dtreport]]






#######################################################################
# look at the mortgage and heloc start around similar date
# be careful, look at the current active only

HELOC = tc[(tc.typecode .== "R") .& (tc.highcr .> 50000) ,:]

Dates.Date("201405", "yyyymm")



##
end_date = Dates.Date("202011", "yyyymm")
HELOC[!,:dtreport] = Dates.Date.(HELOC[!,:dtreport], "yyyymm")

#last report date to today
HELOC[!,:rec_tot0] = [length(x:Month(1):end_date) for x in HELOC[!,:dtreport]]

sort(freqtable(HELOC[!,:rec_tot0]),rev=true)

# check uniqu client id with in 2 report month
length(unique(HELOC[! ,:custref ]))
length(unique(HELOC[HELOC.rec_tot0 .<=2 ,:custref ]))
length(unique(HELOC[HELOC.rec_tot0 .<=3 ,:custref ]))
length(unique(HELOC[((sum(occursin.(l, x)) for x in HELOC[!,:member]) .> 0) .& (HELOC.rec_tot0 .<=3) ,:custref ]))
# and now only look at big banks records
sort(freqtable(HELOC[!,:member]),rev=true)

# get the filtered HELOC 2
l=["CIBC","SCOTIA","TDCT","NATIONAL","RBC","ROYAL","TD CANADA TRUST","BMO","BANK OF MONTREAL","HSBC"]
HELOC2 = HELOC[((sum(occursin.(l, x)) for x in HELOC[!,:member]) .> 0) .& (HELOC.rec_tot0 .<=3),:]

sort(freqtable(HELOC2[!,:member]),rev=true)


# method 2: change member name first then filter




## check mortgage info
mortgage = tc[(tc.typecode .== "M") ,:]

mortgage[!,:dtreport] = Dates.Date.(mortgage[!,:dtreport], "yyyymm")
mortgage[!,:rec_tot0] = [length(x:Month(1):end_date) for x in mortgage[!,:dtreport]]

sort(freqtable(mortgage[!,:rec_tot0]),rev=true)


## 45% of the clients owned the mortgage once, total records are 398237
length(unique(mortgage[!,:custref ]))
length(unique(mortgage[mortgage.rec_tot0 .<=2 ,:custref ]))
length(unique(mortgage[mortgage.rec_tot0 .<=3 ,:custref ]))


###################################################
# now have a file with active mrotgage, and a file with active heloc

mortgage2 = mortgage[mortgage.rec_tot0 .<=3 ,:]
a= innerjoin(mortgage2,HELOC2, on =:custref,makeunique = true)

unique(a[!,:custref])






################################################
# link to customer base
match   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\CUSTOMER_MATCHINGKEY.csv",missingstrings=["","NULL"]))
cus     = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\cus_profile_651k_20210401.csv",missingstrings=["","NULL"]))

# list for the customer who ever had mortgage
list = unique(mortgage[!,:custref ])

list = DataFrame(custref =list)
list[!,:ind] .=1


#
[names(match) eltype.(eachcol(match))]
[names(cus)  eltype.(eachcol(cus))]

join1 = innerjoin(match,cus,on=:PrimaryClientID)

join2 = leftjoin(join1,list, on=:Customer_referece_number =>:custref,makeunique =true)

#

[names(join2)  eltype.(eachcol(join2))]

join2[!,:cat] = [ismissing(x) ? "never mort" : "once mortgage" for x in join2[!,:ind]]


# age bin

age_cuts = [25,35,45,55,65]
join2.age_ca = cut(join2.age_today, age_cuts, extend = true)

freqtable(join2[!,:age_ca],join2[!,:cat])
