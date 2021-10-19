### Pull data from FG ###
# and merge the csvs #


# main function for calling others

import pandas as pd
import requests
import csv
import os
import urllib.parse

apikey = "your apikey here"
# your dir here
directory = r'D:/FGApiParser/Data'

# variables tro download
var_list = 1,2,244

# start and end of timeframe
start_time = "2021-01-01T00:00:00+03:00"
end_time = "2021-09-17T00:00:00+03:00"

basefile = 'basefile.csv'
savefile = 'savefile.csv'

def combine_files(basefile, directory):
    basedata = pd.read_csv(basefile, index_col=0, usecols=[0])
    for entry in os.scandir(directory):
        if (entry.path.endswith('.csv')):
            new_data = pd.read_csv(entry.path, index_col=0, usecols=[0,2])
            # rename "value"
            valuename = entry.name.split('_', 1)
            new_data.columns = [valuename[0]]
            
            combined_data = pd.merge(basedata, new_data, on='start_time', how='outer')
            basedata = combined_data
            print("Combined data from {}".format(valuename[0]))
    return combined_data

def download(directory, var, stime, etime):

    #apikey = apikey

    # quickly fixed vars for sanity
    # example of working:
    """
    https://api.fingrid.fi
    /v1
    /variable
    /53
    /events
    /csv
    ?start_time=2021-09-16T00%3A00%3A00%2B03%3A00
    &end_time=2021-09-17T00%3A00%3A00%2B03%3A00
    """

    url = "https://api.fingrid.fi"
    part1 = "/v1/variable/"
    
    # use var from main
    variable = var
    part2 = "/events/csv?"

    #start_time = "2021-01-01T00:00:00+03:00"
    #end_time = "2021-09-17T00:00:00+03:00"
    # use time from main
    start_time = stime
    end_time = etime

    # encode the url ( : == %3A )
    query = {
        "start_time": start_time,
        "end_time": end_time
        }
    encoded = urllib.parse.urlencode(query)

    combined = part1 + str(variable) + part2  + encoded

    print(url + combined)

    # add the x-api-key (personal)
    headers = {'x-api-key': apikey}

    # GET
    response = requests.get(url + combined, headers=headers)

    print(response)

    # save the data

    # a quick setup for naming
    file_name_end = '{var}_{start}_{end}.csv'.format(
        var=variable, 
        start=start_time[:7], 
        end=end_time[:7]
        )
        
    file_name = directory + '/' + file_name_end


    linecount = 0
    with open(file_name, 'w') as file:
        writer = csv.writer(file, lineterminator='\n')
        
        for line in response.iter_lines():
            linecount += 1
            writer.writerow(line.decode('utf-8').split(','))
            
    print("Saved", linecount, "rows to", file_name)


def main():
    # pull everything from api
    for var in var_list:
        download(directory, var, start_time, end_time)

    # combine all from Data to one merged file
    combine_files(basefile, directory).to_csv(savefile)
    print("Saved file to {}/{}".format(directory, savefile))

if __name__ == "__main__":
    main()