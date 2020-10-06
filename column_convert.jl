#####################################################

using Missings
using DataFrames


###################################################
# testing data
df = DataFrame(A = [1,2,3,1,2,3,1,2,3,missing],
               B =[7,8,missing,9,7,8,9,8,7,9],
               C =["F","M","F","U","F","M","F","M","U","M"])




## method 1 : using dataframe, relace

function mullvldf( df::DataFrame,l::Array{Symbol})
    for z in eachindex(l)
        lvl = levels(df[!,l[z]])
        df[!,Symbol(string(l[z],"_missing"))] = zeros(Int8, size(df[!,l[z]],1))

        for j in eachindex(lvl)
            df[!,Symbol(string(l[z],"_$(lvl[j])"))] = zeros(Int8, size(df[!,l[z]],1))

            for (i,v) in enumerate(df[!,l[z]])
                if ismissing(v)
                    df[!,Symbol(string(l[z],"_missing"))][i] = 1
                end

                if !ismissing(v) && v == lvl[j]
                    df[!,Symbol(string(l[z],"_$(lvl[j])"))][i] = 1
                end
            end
        end
    end
    return df
end

mullvldf(df,[:A,:B,:C])






## method 2: replace on matrix
function mullvldf( df::DataFrame,l::Array{Symbol})
    for z in eachindex(l)
        lvl = levels(df[!,l[z]])
        out = zeros(Int8, size(df[!,l[z]],1),length(lvl)+1)
        col_n = string.(zeros(length(lvl)+1))
        col_n[length(lvl)+1] = string(l[z],"_missing")

        for j in eachindex(lvl)
            for (i,v) in enumerate(df[!,l[z]])
                if ismissing(v)
                    out[i,length(lvl)+1] =1
                end

                if !ismissing(v) && v == lvl[j]
                    out[i,j] = 1
                end
            end

            col_n[j] = string(l[z],"_$(lvl[j])")
        end

        out = DataFrame(out,Symbol.(col_n),makeunique = true)
        df = [df out]
    end
    return df
end


 mullvldf(df,[:A,:B,:C])
