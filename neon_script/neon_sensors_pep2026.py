#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import neonutilities as nu
import pandas as pd
import os

def write_citation(dpnum,siteid,data,savepath):
    """
    Write RELEASE-2026 citation to savepath for a downloaded NEON data product

    Parameters
    --------
    dpnum: 5 letter NEON Data Product code (e.g., '00130' for DP4.00130.001)
    siteid: Four letter NEON site ID.
    data: NEON download package output from nu.load_by_product()
    savepath: The filepath of the output file.

    Return
    --------
    CSV files containing daily values of NEON sensor data

    Created on Fri Feb 27 2026

    @author: Zachary Nickerson
    """
    citations = [value for key, value in data.items() if 'citation_'+dpnum in key.lower()]
    with open(savepath+siteid+"_citation_"+dpnum+".txt", "w") as citation_file:
        print(citations, file=citation_file)
        
def neon_dwnld_sum_daily(savepath,siteid,startmonth,endmonth,timeutc):
    """
    helper function for the PEP2026 workshop that downloads multiple NEON
    sensor data streams for a site and subsets them to a single daily timestamp
    defined by the user. The NEON data products downloaded are Water quality
    (DP1.20288.001) and Continuous discharge (DP4.00130.001). The NEON data
    downloaded and used for PEP2026 were part of NEON RELEASE-2026. No
    provisional data were used.

    Parameters
    --------
    savepath: The filepath of the output file.
    siteid: Four letter NEON site ID.
    startmonth: Start month of NEON Data Portal query (YYYY-MM)
    endmonth: End month of NEON Data Portal query (YYYY-MM)
    timeutc: The timestamp (HH:MM UTC) at which to subset to daily values

    Return
    --------
    CSV files containing daily values of NEON sensor data

    Created on Fri Feb 27 2026

    @author: Zachary Nickerson
    """
    
    # Parse the target time (timeutc)
    target_hour, target_minute = map(int, timeutc.split(':'))
        
    # Download the NEON Water quality data product (DP1.20288.001)
    try:
        # Download the data
        data = nu.load_by_product(
            dpid="DP1.20288.001",
            site=siteid,
            startdate=startmonth,
            enddate=endmonth,
            release="RELEASE-2026",
            package="basic",
            check_size=False,
            token=os.environ.get("NEON_TOKEN")
        )
        
        # If the download is successful
        if isinstance(data,dict):
            
            # Write out citations to savepath
            write_citation(dpnum="20288",
                           siteid=siteid,
                           data=data,
                           savepath=savepath)
            
            # Get the water quality data frame
            waq_instantaneous=data["waq_instantaneous"]

            # Subset to sensor located at S2
            s2locs = ['102','110','112','132']
            waq_instantaneous = waq_instantaneous[waq_instantaneous['horizontalPosition'].isin(s2locs)]

            # NEON Water quality data is instantaneous (1-min temporal res)
            # To smooth, take a 15-min average around timeutc

            # Convert endDateTime to datetime if it's not already
            waq_instantaneous['endDateTime'] = pd.to_datetime(waq_instantaneous['endDateTime'])
            
            # Create a function to check if a timestamp is within 7.5 minutes of target time
            def is_within_window(dt, target_h, target_m):
                # Convert datetime to minutes from start of day
                dt_minutes = dt.hour * 60 + dt.minute + dt.second / 60.0
                target_minutes = target_h * 60 + target_m
                
                # Calculate time difference (handle day boundary)
                diff = abs(dt_minutes - target_minutes)
                if diff > 12 * 60:  # If difference is more than 12 hours, check the other direction
                    diff = 24 * 60 - diff
                
                return diff <= 7.5  # Within 7.5 minutes
            
            # Filter data to only include timestamps within 7.5 minutes of target time each day
            waq_filtered = waq_instantaneous[
                waq_instantaneous['endDateTime'].apply(
                    lambda x: is_within_window(x, target_hour, target_minute)
                )
            ].copy()
            
            # Add datetime column for grouping at the requested UTC time
            waq_filtered['date'] = pd.to_datetime(
                waq_filtered['endDateTime'].dt.strftime('%Y-%m-%d') + f' {timeutc}',
                format='%Y-%m-%d %H:%M'
            )
            
            # Keep requested non-numeric metadata columns in output
            metadata_cols = ['domainID', 'siteID', 'horizontalPosition', 'release']
            existing_meta = [c for c in metadata_cols if c in waq_filtered.columns]

            # Identify numeric columns for averaging (exclude metadata if present)
            numeric_cols = waq_filtered.select_dtypes(include=['number']).columns.tolist()
            numeric_cols = [c for c in numeric_cols if c not in existing_meta]

            # Group by date, average numeric fields, and keep metadata columns
            agg_map = {c: 'mean' for c in numeric_cols}
            agg_map.update({c: 'first' for c in existing_meta})
            daily_averages = waq_filtered.groupby('date', as_index=False).agg(agg_map)
            daily_averages = daily_averages[['date'] + existing_meta + [c for c in numeric_cols if c in daily_averages.columns]]
            
            # Write daily summary file to csv
            daily_averages.to_csv(savepath+siteid+"_daily_20288.csv", index=False)
        
    except Exception as e:
        print(f"Error downloading data: {e}")
        return None
    
    # Download the NEON Water quality data product (DP1.20288.001)
    try:
        # Download the data
        data = nu.load_by_product(
            dpid="DP4.00130.001",
            site=siteid,
            startdate=startmonth,
            enddate=endmonth,
            release="RELEASE-2026",
            package="basic",
            check_size=False,
            token=os.environ.get("NEON_TOKEN")
        )
        
        # If the download is successful
        if isinstance(data,dict):
            
            # Write out citations to savepath
            write_citation(dpnum="00130",
                           siteid=siteid,
                           data=data,
                           savepath=savepath)
            
            # Get the continuous discharge data frame
            csd_15_min=data["csd_15_min"]
            
            # Subset to timeutc
            # Convert endDateTime to datetime if it's not already
            csd_15_min['endDateTime'] = pd.to_datetime(csd_15_min['endDateTime'])
            
            # Filter to keep only records with time component matching timeutc
            csd_filtered = csd_15_min[
                (csd_15_min['endDateTime'].dt.hour == target_hour) & 
                (csd_15_min['endDateTime'].dt.minute == target_minute)
            ].copy()
            
            # Write discharge summary file to csv
            csd_filtered.to_csv(savepath+siteid+"_daily_00130.csv", index=False)            
            
    except Exception as e:
        print(f"Error downloading data: {e}")
        return None
    
    success="Both data products successfully summarized and saved to savapath"
    return(print(success))

def main():
    """
    Main function to download and store all summarized NEON data products
    """
    # Create a dictionary of NEON sites, date ranges, and timestamps to loop through
    site_inputs = {
        "LEWI": {"startmonth": "2023-10", "endmonth": "2024-09", "timeutc": "16:00"},
        "COMO": {"startmonth": "2023-10", "endmonth": "2024-09", "timeutc": "18:00"},
        "LECO": {"startmonth": "2023-10", "endmonth": "2024-09", "timeutc": "16:00"},
        "MCRA": {"startmonth": "2023-10", "endmonth": "2024-09", "timeutc": "19:00"},
    }

    # Loop through the dictionary and run the neon_dwnld_sum_daily function
    for siteid, params in site_inputs.items():
        try:
            print(f"Processing site {siteid} with parameters: {params}")
            neon_dwnld_sum_daily(
                savepath="C:/Users/nickerson/Documents/GitHub/PEP2026/neon_script/data/",
                siteid=siteid,
                startmonth=params["startmonth"],
                endmonth=params["endmonth"],
                timeutc=params["timeutc"]
            )
            
        except Exception as e:
            print(f"Error processing site {siteid}: {e}")
        
    success="All sites processed successfully"
    return(print(success))

if __name__ == "__main__":
    main()

# End