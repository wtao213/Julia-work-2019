ENV["COLUMNS"]=240
ENV["LINES"] = 100

using DataFrames




###
df =  DataFrame(a = [1,2,3,3,3,4,5,6,7,7,7,10,11,11,11,12,13,14,14,14,14]
               , b = [1,2,3,4,5,6,6,7,8,7,9,10,11,12,13,12,12,12,13,14,14])


function finda(df::DataFrame,a,b,l)
    group     = df[∈(l).(df[!,a]) ,:]
    df_remain = df[∉(l).(df[!,a]),:]

        # find is there any remaining rows contain b
    fb = unique(group[!,b])
    find_byb  = df_remain[∈(fb).(df_remain[!,b]) ,:]
    df_remain = df_remain[∉(fb).(df_remain[!,b]),:]
    l  = [l;unique(find_byb[!,a])]

    return group,find_byb,df_remain,l

end

function test(group,find_byb,df_remain,l,a,b)
    if length(find_byb[!,1]) == 0
    dfn = DataFrame(a = Int64[], b = Int64[], grid =Int64[], rlen =String[])   
    group[!,:rlen] .= "$(length(unique(group[!,a]))) : $(length(unique(group[!,b])))" 
    dfn = [dfn;group]
    dfn[!,:grid] .= l[1]
    return dfn,df_remain

   else
    group = [group;find_byb]
    group2,find_byb,df_remain,l=finda(df_remain,a,b,l)
    group = [group;group2]
    test(group,find_byb,df_remain,l,a,b)
   end
end


function remain(dfn,df_remain,a,b)
    if length(df_remain[!,1]) == 0 && return dfn
    else
        group,find_byb,df_remain,l=finda(df_remain,a,b,[df_remain[!,a][1]])
        dfn2,df_remain =test(group,find_byb,df_remain,l,a,b)
        dfn = [dfn;dfn2]
        remain(dfn,df_remain,a,b)
    end
end


###############################################
#
function groups(df,a,b)
    # remove duplicate first
    unique!(df)
    sort!(df,a)

    #generate column with group id
    df[!,:grid] = df[!,a]

    # start part
    s = df[!,a][1]

    group,find_byb,df_remain,l=finda(df,a,b,s)
    dfn,df_remain =test(group,find_byb,df_remain,l,a,b)

    dfo = remain(dfn,df_remain,a,b)
    return dfo

end


groups(df,:a,:b)
